# Sharded Lock

Mapa concurrente (`ShardedMap`) que reparte un mapa en **N shards**, cada uno con su propio `sync.RWMutex` y su propio `map[K]V`.

Ejemplo de carga: **1000 lecturas + 50 escrituras** concurrentes.

---

## Enfoque 1: un solo `RWMutex` para todo el mapa

Un `map[K]V` protegido por **un único** `sync.RWMutex`. Las lecturas comparten lock; las escrituras lo toman en exclusiva.

```mermaid
flowchart TB
  subgraph load["Carga: 1000 Get + 50 Set"]
    R["1000× Get(key)<br/>RLock()"]
    W["50× Set(key)<br/>Lock()"]
  end

  RW["sync.RWMutex<br/>1 lock para todo"]
  MAP["map[K]V<br/>todas las claves"]

  R --> RW
  W --> RW
  RW --> MAP

  subgraph bottleneck["Cuello de botella global"]
    B1["❌ 50 escrituras serializadas<br/>sobre el mismo lock"]
    B2["❌ cada Set bloquea los 1000 Get<br/>del mapa entero"]
    B3["❌ claves distintas compiten igual<br/>porque comparten 1 RWMutex"]
  end

  RW -.-> bottleneck
```

```mermaid
sequenceDiagram
  participant G1 as Get(42)
  participant G2 as Get(99)
  participant G3 as Get(7)
  participant RW as RWMutex global
  participant S as Set(100)

  par lecturas en paralelo
    G1->>RW: RLock()
    G2->>RW: RLock()
    G3->>RW: RLock()
  end

  S->>RW: Lock() — espera lectores activos
  Note over RW: todo el mapa congelado<br/>1000 Get en cola

  RW-->>S: lock exclusivo
  S->>RW: Unlock()

  G1->>RW: RUnlock()
  G2->>RW: RUnlock()
  G3->>RW: RUnlock()
```

| Operación | Lock | Efecto |
|-----------|------|--------|
| `Get` | `RLock()` | Paralelo entre lectores, **pero compite con writers** |
| `Set` / `Delete` | `Lock()` | Exclusivo — **pausa todas las lecturas y escrituras del mapa** |

Con 1000 lecturas y 50 escrituras, el `RWMutex` global sigue siendo un cuello de botella: cada escritura detiene temporalmente **todo** el mapa.

---

## Enfoque 2: Sharded Lock (`ShardedMap`)

El mapa se divide en **N buckets**. Cada bucket tiene su propio `RWMutex` y su propio `map[K]V`. La clave se enruta con `hash(key) % N`.

```mermaid
flowchart TB
  subgraph load2["Carga: 1000 Get + 50 Set"]
    R2["1000× Get(key)"]
    W2["50× Set(key)"]
  end

  HASH["hash(key) % N"]

  subgraph sm["ShardedMap · N = 64"]
    direction LR

    subgraph s0["Shard 0"]
      RW0["RWMutex"]
      M0["map[K]V"]
      RW0 --- M0
    end

    subgraph s1["Shard 1"]
      RW1["RWMutex"]
      M1["map[K]V"]
      RW1 --- M1
    end

    subgraph s2["Shard 2"]
      RW2["RWMutex"]
      M2["map[K]V"]
      RW2 --- M2
    end

    subgraph sn["Shard 63"]
      RWn["RWMutex"]
      Mn["map[K]V"]
      RWn --- Mn
    end
  end

  R2 --> HASH
  W2 --> HASH

  HASH -->|"Get(42)"| RW0
  HASH -->|"Set(99)"| RW1
  HASH -->|"Get(7)"| RW2
  HASH -->|"Get(100)"| RW1
```

```mermaid
sequenceDiagram
  participant G1 as Get(42) → shard 0
  participant G2 as Get(99) → shard 1
  participant G3 as Get(7) → shard 2
  participant S as Set(100) → shard 1

  par lecturas en shards distintos
    G1->>G1: RLock shard 0
    G2->>G2: RLock shard 1
    G3->>G3: RLock shard 2
  end

  Note over G1,G3: ✓ avanzan en paralelo<br/>sin bloquearse entre sí

  S->>G2: Lock() shard 1 — solo bloquea shard 1
  Note over G2: Get(99) y Set(100)<br/>compiten en shard 1

  Note over G1,G3: ✓ shard 0 y 2<br/>siguen leyendo sin pausa
```

| Operación | Lock | Alcance |
|-----------|------|---------|
| `Get(key)` | `RLock()` | Solo `shards[hash(key) % N]` |
| `Set(key, v)` | `Lock()` | Solo `shards[hash(key) % N]` |
| `Delete(key)` | `Lock()` | Solo `shards[hash(key) % N]` |

Con la misma carga (1000 reads + 50 writes), las 50 escrituras se reparten entre shards (~1 por shard con N=64). Una escritura **solo pausa el bucket afectado**, no el mapa completo.
