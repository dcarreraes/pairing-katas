# Análisis: Blind Window

## El Problema

Las reservas creadas **mientras el worker estaba caído no reciben notificación**.

### Causa Raíz

El worker usa `update_seq` para saber desde dónde reanudar. Cuando se reinicia:

```
Worker cae → Llegan cambios (seq 30420, 30421) → Worker reinicia
                                                    ↓
                                      Inicia desde update_seq ACTUAL
                                      (que es 30421 ahora)
                                      ↓
                                      PIERDE cambios 30420-30421
```

Sin checkpoint persistente, pierde la referencia entre reinicios.

### Impacto

- Reserva creada offline → nunca se notifica
- Cambio de estado offline → nunca se procesa
- Solo se recupera si el doc se modifica después del restart

---

## Escenario del Incidente

```
1. Worker ACTIVO: procesa cambios desde seq 0
2. Worker CAE: nuevas reservas llegan (seq 30420, 30421)
3. Worker REINICIA: lee update_seq=30421, comienza desde ahí
4. RESULTADO: reservas 30420-30421 quedan atrás, nunca se procesan
```

---

## Requisitos para Solución

✅ Guardar checkpoint entre reinicios  
✅ Procesar cambios históricos al primer arranque  
✅ Recuperar fallos de envío de email (estado durable)  
✅ Todos los tests pasan
