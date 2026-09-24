# Solución: Checkpoint + Barrido de Huérfanos

## Estrategia

**3 capas de recuperación:**

1. **Checkpoint persistente** - Guardar/recuperar `lastSeq` en BD
2. **Primer arranque desde seq 0** - Procesar todo, sin ventana ciega
3. **Barrido de huérfanos** - Recuperar fallos de email al arrancar

---

## Implementación

### 1. Checkpoint en `changes-feed-worker.ts`

**Guardar**: Después de procesar cambios (línea 126-132)
```typescript
if (processedCount > 0) {
  lastSeq = changes.last_seq;
  saveCheckpoint(changes.last_seq);
}
```

**Recuperar**: En `start()` (línea 153-177)
```typescript
try {
  const checkpoint = await db.get(getCheckpointId());
  lastSeq = checkpoint.lastSeq;
} catch (err) {
  if (status === 404) {
    lastSeq = 0; // Primer arranque: procesa todo
  }
}
```

ID único por consumidor: `_local/changes-feed-checkpoint:[logPrefix]`

### 2. Barrido de Huérfanos en `reservations-listener.ts`

**Función**: `recoverOrphanReservations()` (línea 142-197)
```typescript
// Busca docs sin email flags
- scheduled sin scheduledEmailSentAt
- occupied sin occupiedEmailSentAt
// Procesa usando handlers existentes
```

**Llamada**: En `start()` antes del feed (línea 216)

---

## Trade-offs

| Aspecto | Decisión | Motivo |
|---------|----------|--------|
| **Primer arranque** | seq 0 (lee todo) | Completitud > Eficiencia |
| **Recuperación de fallos** | Estado durable (email flags) | No depender de memoria |
| **Barrido** | Best-effort | No bloquear si alguno falla |

---

## Validación

✅ 11/11 tests pasan  
✅ TypeScript compliance  
✅ Logs esperados en primer y segundo arranque
