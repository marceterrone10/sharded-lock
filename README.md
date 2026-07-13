# Sharded Lock

Un **sharded lock** reparte la contención de un mutex global entre varios locks independientes (_shards_). Cada clave se mapea a un shard con un hash, de modo que operaciones sobre claves distintas pueden avanzar en paralelo.

```mermaid
flowchart TB
  subgraph clients["Clientes concurrentes"]
    C1["goroutine A<br/>key = user:42"]
    C2["goroutine B<br/>key = user:99"]
    C3["goroutine C<br/>key = order:7"]
  end

  H["hash(key) % N"]

  subgraph pool["ShardedLock · N shards"]
    direction LR
    S0["Shard 0<br/>mutex"]
    S1["Shard 1<br/>mutex"]
    S2["Shard 2<br/>mutex"]
    SN["Shard N-1<br/>mutex"]
  end

  C1 --> H
  C2 --> H
  C3 --> H

  H -->|"hash → 0"| S0
  H -->|"hash → 2"| S2
  H -->|"hash → 1"| S1

  S0 --> Crit0["sección crítica<br/>solo keys → shard 0"]
  S1 --> Crit1["sección crítica<br/>solo keys → shard 1"]
  S2 --> Crit2["sección crítica<br/>solo keys → shard 2"]
```

## Flujo

1. La goroutine recibe una `key`.
2. Calcula `shard = hash(key) % N`.
3. Adquiere el mutex de ese shard (no el de todos).
4. Ejecuta la sección crítica y libera el lock.

Claves distintas con distinto shard no se bloquean entre sí; solo compiten las que caen en el mismo shard.
