import {
  type ChangesFeedWorker,
  createChangesFeedWorker,
} from "./changes-feed-worker";
import type {
  Database,
  EmailService,
  Logger,
  ReservationDoc,
} from "./types";

export interface ReservationsListenerDeps {
  reservationsDb: Database<ReservationDoc>;
  emailService: EmailService;
  logger: Logger;
  pollIntervalMs?: number;
}

export interface ReservationsListener {
  start(): Promise<void>;
  stop(): Promise<void>;
  isActive(): boolean;
}

/**
 * Create reservations listener.
 *
 * Escucha el changes feed de la base de reservas y dispara dos correos:
 * - reserva nueva en estado `scheduled` → correo al equipo operativo
 * - transición a `occupied` (maleta entregada) → correo a quien reservó
 */
export function createReservationsListener(
  deps: ReservationsListenerDeps,
): ReservationsListener {
  const { reservationsDb, emailService, logger, pollIntervalMs } = deps;

  let isActive = false;
  let feedWorker: ChangesFeedWorker | null = null;

  // Per-reservation deduplication: track individual reservation IDs already notified
  const processedReservations = new Set<string>();
  const processedOccupiedReservations = new Set<string>();

  /**
   * Mark reservations as notified in CouchDB for cross-process deduplication.
   */
  async function markAsNotified(
    docId: string,
    field: "scheduledEmailSentAt" | "occupiedEmailSentAt",
  ): Promise<void> {
    try {
      const doc = await reservationsDb.get(docId);
      if (!doc[field]) {
        await reservationsDb.put({
          ...doc,
          [field]: new Date().toISOString(),
        });
      }
    } catch (error) {
      // 409 conflict means another process already marked it — safe to ignore
      const status = (error as { status?: number }).status;
      if (status !== 409) {
        logger.debug(
          `[Notifications] Could not mark ${docId} as notified: ${error}`,
        );
      }
    }
  }

  /**
   * Handle new scheduled reservation
   */
  async function handleScheduledReservation(doc: ReservationDoc): Promise<void> {
    if (processedReservations.has(doc._id)) {
      logger.debug(
        `[Notifications] Skipping already processed reservation: ${doc._id}`,
      );
      return;
    }
    processedReservations.add(doc._id);

    logger.info(
      `[Notifications] New scheduled reservation detected: ${doc._id}`,
    );

    try {
      await emailService.sendReservationEmail({
        reservationId: doc._id,
        containerId: doc.containerId,
        locationId: doc.locationId,
        createdBy: doc.createdBy,
      });
      await markAsNotified(doc._id, "scheduledEmailSentAt");
      logger.info(
        `[Notifications] Reservation email sent successfully for ${doc._id}`,
      );
    } catch (error) {
      processedReservations.delete(doc._id);
      logger.error("[Notifications] Failed to send reservation email", {
        error,
        reservationId: doc._id,
      });
    }
  }

  /**
   * Handle transition to "occupied": notify the reservation creator.
   */
  async function handleOccupiedTransition(doc: ReservationDoc): Promise<void> {
    // Skip if this occupied transition was already processed
    if (processedOccupiedReservations.has(doc._id)) {
      logger.debug(
        `[Notifications] Skipping already processed occupied transition: ${doc._id}`,
      );
      return;
    }
    processedOccupiedReservations.add(doc._id);

    logger.info(
      `[Notifications] Reservation ${doc._id} transitioned to occupied`,
    );

    try {
      await emailService.sendOccupiedEmail({
        reservationId: doc._id,
        containerId: doc.containerId,
        occupiedBy: doc.occupiedBy ?? "N/A",
      });
      await markAsNotified(doc._id, "occupiedEmailSentAt");
      logger.info(
        `[Notifications] Occupied email sent successfully for ${doc._id}`,
      );
    } catch (error) {
      processedOccupiedReservations.delete(doc._id);
      logger.error("[Notifications] Failed to send occupied email", {
        error,
        reservationId: doc._id,
      });
    }
  }

  async function recoverOrphanReservations(): Promise<void> {
    try {
      // Recover scheduled reservations that failed to send email
      const scheduledOrphans = await reservationsDb.find({
        selector: {
          type: "reservation",
          status: "scheduled",
          scheduledEmailSentAt: { $exists: false },
        },
      });

      if (scheduledOrphans.docs.length > 0) {
        logger.info(
          `[Notifications] Recovering ${scheduledOrphans.docs.length} scheduled orphan(s)`,
        );
        for (const doc of scheduledOrphans.docs) {
          try {
            await handleScheduledReservation(doc);
          } catch (err) {
            logger.error(
              `[Notifications] Failed to recover scheduled orphan: ${doc._id}`,
              err,
            );
          }
        }
      }

      // Recover occupied transitions that failed to send email
      const occupiedOrphans = await reservationsDb.find({
        selector: {
          type: "reservation",
          status: "occupied",
          occupiedEmailSentAt: { $exists: false },
        },
      });

      if (occupiedOrphans.docs.length > 0) {
        logger.info(
          `[Notifications] Recovering ${occupiedOrphans.docs.length} occupied orphan(s)`,
        );
        for (const doc of occupiedOrphans.docs) {
          try {
            await handleOccupiedTransition(doc);
          } catch (err) {
            logger.error(
              `[Notifications] Failed to recover occupied orphan: ${doc._id}`,
              err,
            );
          }
        }
      }
    } catch (err) {
      logger.error("[Notifications] Orphan recovery failed", err);
      // Continue anyway: orphan recovery is best-effort
    }
  }

  async function start(): Promise<void> {
    if (isActive) {
      logger.warn("[Notifications] Reservations listener is already active");
      return;
    }

    logger.info("[Notifications] Starting reservations listener...");
    isActive = true;

    try {
      // Test connection
      const info = await reservationsDb.info();
      logger.info(
        `[Notifications] Connected to reservations database: ${info.db_name} (${info.doc_count} docs)`,
      );

      // Recover any reservations that failed to send emails (orphans)
      await recoverOrphanReservations();

      // The changes-feed loop (checkpoint recovery, polling, design-doc skip,
      // fault tolerance) is the shared createChangesFeedWorker primitive;
      // this listener keeps the per-change processing.
      //
      // On first startup (no saved checkpoint), the worker re-reads from seq 0:
      // the entire history. This costs processing time but ensures no changes
      // are missed—especially reservations that arrived before the listener
      // was deployed. Duplicates are prevented by durable doc markers
      // (scheduledEmailSentAt, occupiedEmailSentAt), not by memory.
      feedWorker = createChangesFeedWorker<ReservationDoc>({
        db: reservationsDb,
        logger,
        logPrefix: "[Notifications]",
        pollIntervalMs,
        onChange: async (change) => {
          if (change.doc && !change.deleted) {
            try {
              const doc = change.doc;
              const currentStatus = doc.status;
              const isNewDocument = doc._rev?.startsWith("1-");

              // Handle occupied reservation (by document state, not in-memory transition)
              if (currentStatus === "occupied" && !doc.occupiedEmailSentAt) {
                await handleOccupiedTransition(doc);
              }

              // Handle new scheduled reservations
              if (
                isNewDocument &&
                currentStatus === "scheduled" &&
                !doc.scheduledEmailSentAt
              ) {
                await handleScheduledReservation(doc);
              }
            } catch (error) {
              logger.error("[Notifications] Error processing reservation", {
                error,
                reservationId: change.id,
              });
            }
          } else if (change.deleted) {
            logger.info(`[Notifications] Reservation deleted: ${change.id}`);
          }
        },
      });
      await feedWorker.start();

      logger.info("[Notifications] Reservations listener started successfully");
    } catch (error) {
      isActive = false;
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      logger.error(
        `[Notifications] Failed to start reservations listener: ${errorMessage}`,
        { error },
      );
      throw new Error(`Failed to start reservations listener: ${errorMessage}`);
    }
  }

  async function stop(): Promise<void> {
    if (!isActive) {
      logger.warn("[Notifications] Reservations listener is not active");
      return;
    }

    logger.info("[Notifications] Stopping reservations listener...");

    processedReservations.clear();
    processedOccupiedReservations.clear();

    // Stop polling
    if (feedWorker) {
      feedWorker.stop();
      feedWorker = null;
    }

    isActive = false;
    logger.info("[Notifications] Reservations listener stopped");
  }

  return {
    start,
    stop,
    isActive: () => isActive,
  };
}
