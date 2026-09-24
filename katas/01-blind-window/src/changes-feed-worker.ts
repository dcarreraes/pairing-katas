import type { BaseDoc, ChangeRow, Database, Logger } from "./types";

/** Checkpoint document structure. */
interface Checkpoint {
  _id: string;
  lastSeq: string | number;
}

/** Delay between changes-feed polls (also the delay before the first one). */
export const DEFAULT_CHANGES_POLL_INTERVAL_MS = 5_000;
/** Max changes fetched per poll. */
export const DEFAULT_CHANGES_BATCH_LIMIT = 100;
/** Offset to re-deliver last change from previous batch. */
export const CHECKPOINT_OFFSET = 1;

export interface ChangesFeedWorkerDeps<T extends BaseDoc> {
  db: Database<T>;
  /**
   * Called once per non-design change, including deletions (`change.doc` is
   * absent for those). Filtering and processing are the consumer's business;
   * a throw is logged and the rest of the batch still runs.
   */
  onChange: (change: ChangeRow<T>) => void | Promise<void>;
  logger: Logger;
  /** Log-line identity per consumer, e.g. `"[Notifications]"`. */
  logPrefix: string;
  pollIntervalMs?: number;
  limit?: number;
  includeDocs?: boolean;
  /**
   * Checkpoint to start from. Default: tries to recover a persisted
   * checkpoint from the DB; if none exists (first startup), starts from
   * seq 0 to re-read the full history. This ensures no changes are missed,
   * but incurs a cost on first startup: every prior change is processed.
   * Consumers must rely on durable doc state to avoid duplicates, not on
   * in-memory checkpoints.
   */
  initialSince?: string | number;
}

/**
 * CouchDB changes-feed listener with a `since` checkpoint. Extracted from the
 * poll loop the notification listeners kept duplicating.
 *
 * Fault tolerance:
 * - A failed poll is logged and retried on the next tick; `lastSeq` does not
 *   advance, so no change is lost.
 * - A throwing `onChange` is logged and the batch continues — one bad doc
 *   never stalls the feed. The batch's `last_seq` is still checkpointed, so
 *   a persistently-throwing doc is skipped, not retried forever.
 */
export function createChangesFeedWorker<T extends BaseDoc>(
  deps: ChangesFeedWorkerDeps<T>,
) {
  const {
    db,
    onChange,
    logger,
    logPrefix,
    pollIntervalMs = DEFAULT_CHANGES_POLL_INTERVAL_MS,
    limit = DEFAULT_CHANGES_BATCH_LIMIT,
    includeDocs = true,
    initialSince,
  } = deps;
  let active = false;
  let lastSeq: string | number = 0;
  let pollTimeout: ReturnType<typeof setTimeout> | null = null;

  function getCheckpointId(): string {
    return `_local/changes-feed-checkpoint:${logPrefix}`;
  }

  async function saveCheckpoint(seq: string | number): Promise<void> {
    try {
      try {
        const checkpoint = await db.get(getCheckpointId());
        await db.put({
          ...checkpoint,
          lastSeq: seq,
        });
      } catch (err) {
        const status = (err as { status?: number }).status;
        if (status === 404) {
          await db.put({
            _id: getCheckpointId(),
            lastSeq: seq,
          } as Checkpoint);
        } else {
          throw err;
        }
      }
    } catch (err) {
      logger.error(`${logPrefix} Failed to save checkpoint: ${err}`);
    }
  }

  async function poll(): Promise<void> {
    if (!active) return;
    try {
      const changes = await db.changes({
        since: lastSeq,
        include_docs: includeDocs,
        limit,
      });

      if (changes.results.length > 0) {
        logger.debug(`${logPrefix} Polled ${changes.results.length} change(s)`);
      }

      let processedCount = 0;
      for (const change of changes.results) {
        if (change.id.startsWith("_design/") || change.id.startsWith("_local/")) continue;
        processedCount++;
        try {
          await onChange(change);
        } catch (err) {
          logger.error(
            `${logPrefix} Error processing change ${change.id}`,
            err,
          );
        }
      }

      // Update lastSeq and save checkpoint only if we processed real changes
      if (processedCount > 0) {
        lastSeq = changes.last_seq;
        // Save checkpoint without blocking poll
        saveCheckpoint(changes.last_seq).catch((err) => {
          logger.error(
            `${logPrefix} Unexpected error saving checkpoint: ${err}`,
          );
        });
      }
    } catch (err) {
      logger.error(`${logPrefix} Error polling changes feed`, err);
    }

    if (active) {
      pollTimeout = setTimeout(() => {
        poll().catch((err) =>
          logger.error(`${logPrefix} Changes poll failed`, err),
        );
      }, pollIntervalMs);
    }
  }

  async function start(): Promise<void> {
    if (active) return;
    active = true;

    if (initialSince !== undefined) {
      lastSeq = initialSince;
    } else {
      try {
        // Try to recover saved checkpoint
        const checkpoint = await db.get(getCheckpointId()) as Checkpoint;
        const checkpointSeq = checkpoint.lastSeq;
        // Subtract CHECKPOINT_OFFSET to re-deliver the last change from previous batch
        lastSeq = typeof checkpointSeq === "number"
          ? Math.max(0, checkpointSeq - CHECKPOINT_OFFSET)
          : 0;
        logger.debug(
          `${logPrefix} Recovered checkpoint from database: ${lastSeq}`,
        );
      } catch (err) {
        const status = (err as { status?: number }).status;
        if (status === 404) {
          // No checkpoint: start from seq 0 to process all history.
          // This ensures no changes are lost, even those created before
          // the first startup. Cost: slow first startup (re-reads entire DB).
          lastSeq = 0;
          logger.debug(
            `${logPrefix} No checkpoint found, starting from seq: 0 (reading full history)`,
          );
        } else {
          throw err;
        }
      }
    }
    logger.info(
      `${logPrefix} Changes feed polling from seq: ${lastSeq} (every ${pollIntervalMs}ms)`,
    );

    pollTimeout = setTimeout(() => {
      poll().catch((err) =>
        logger.error(`${logPrefix} Changes poll failed`, err),
      );
    }, pollIntervalMs);
  }

  function stop(): void {
    active = false;
    if (pollTimeout) {
      clearTimeout(pollTimeout);
      pollTimeout = null;
    }
  }

  return {
    start,
    stop,
    get lastSeq() {
      return lastSeq;
    },
    get isActive() {
      return active;
    },
  };
}

export type ChangesFeedWorker = ReturnType<typeof createChangesFeedWorker>;
