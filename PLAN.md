# 🛡️ P1 — Real-time Fraud Detection Platform

> **Plan de proyecto** (vive en este repo como `PLAN.md`). **Estado:** ✅ Plan completo (12/12 módulos), listo para implementar en la fase F3 del plan de Learning.
> Stack: Kafka (KRaft) · Flink SQL · Redis · XGBoost → ONNX Runtime · PyTorch (challenger en shadow) · MLflow · PostgreSQL · FastAPI · Prometheus + Grafana · OpenTelemetry · LLM Gateway (`llm-gateway`, Python) · Docker Compose
> **v2 (§13):** Evidently (drift) · backtesting como job batch en contenedor (KFP local) sobre MinIO/S3 versionado · Pandas · `judgekit` (auditoría de explicaciones)
> **v3 (§14):** gRPC (`grpc.aio`, protobuf, `buf`, deadlines, health, reflection, interceptores OTel) para el scoring síncrono
> Gasto: **$0** (todo local).

## Módulos del plan
| # | Sección | Estado |
|---|---|---|
| 1 | Visión, problema y frase del CV | ✅ |
| 2 | Arquitectura macro y flujo de datos | ✅ |
| 3 | Componentes (uno por pieza, de lo conceptual a lo técnico) | ✅ |
| 4 | Modelo: datos sintéticos, features, entrenamiento, champion/challenger | ✅ |
| 5 | Metodología de medición y pruebas de carga | ✅ |
| 6 | Observabilidad | ✅ |
| 7 | Pruebas de falla y resiliencia | ✅ |
| 8 | Presupuesto de recursos (8 GB / `⏳ 16GB`) | ✅ |
| 9 | Ejecución local y alternativas de despliegue | ✅ |
| 10 | Estructura del repo y del README | ✅ |
| 11 | Hitos de implementación y criterios de aceptación | ✅ |
| 12 | Riesgos y pendientes | ✅ |
| 13 | **v2 — Absorción de los proyectos del CV** (Evidently, backtesting en contenedor sobre MinIO, auditoría de explicaciones) | ✅ v2 |
| 14 | **v3 — Scoring síncrono con gRPC** (unario, streaming bidireccional, deadlines, E11 REST vs gRPC) | ✅ v3 |

## Regla del README
README progresivo: **contexto teórico, conceptual y macro primero**; en cada componente, el detalle técnico al final. Incluye cómo funciona, los pasos para ejecutarlo y las alternativas de ejecución o despliegue (local primero).

---

## 1. Visión, problema y frase del CV

### 1.1 El problema (contexto de negocio)
Cuando alguien paga con tarjeta en línea, el banco o la pasarela tiene que **aprobar, pedir verificación o bloquear** el pago mientras el usuario espera frente al checkout. Toda la autorización suele tener un presupuesto de unos pocos cientos de milisegundos, y la verificación de fraude solo recibe una parte. Por eso la meta de **p95 ≤ 80 ms** para nuestra etapa.

El fraude se detecta sobre todo por **comportamiento reciente**, no por un pago aislado:
- **Ráfagas:** 8 pagos en 2 minutos cuando el usuario suele hacer 2 al día.
- **Cambio de país o dispositivo:** un pago en Colombia y, 5 minutos después, otro en Rumania.
- **Montos atípicos:** un monto 10 veces mayor que su promedio de los últimos 30 días.
- **Prueba de tarjetas (card testing):** muchos montos pequeños seguidos.

Estas señales son **features de ventana de tiempo** (*velocity features*) que deben estar actualizadas **en el momento del pago**. Un job batch nocturno llega tarde. Por eso hace falta *streaming*: Kafka transporta, Flink calcula las ventanas evento por evento, Redis las sirve y XGBoost decide en milisegundos.

### 1.2 Qué construimos
Una plataforma local y reproducible con un solo comando:
1. Un **generador** de pagos sintéticos a una tasa controlada, con patrones de fraude inyectados y etiquetados.
2. **Kafka** transporta los eventos.
3. **Flink SQL** calcula las features por usuario en ventanas y emite el evento enriquecido.
4. **Redis** guarda el estado online de cada usuario.
5. Un **scorer** con XGBoost en ONNX Runtime decide `APPROVE`, `REVIEW` o `BLOCK` y publica la decisión.
6. Un **challenger** en PyTorch puntúa en shadow, sin afectar las decisiones.
7. Un **explicador asíncrono** con SHAP más un LLM local, vía el gateway, explica los casos `REVIEW` y `BLOCK`, fuera del camino crítico.
8. **Observabilidad** de punta a punta: OTel, Prometheus y Grafana.
9. Una **suite de carga** que produce la tabla de resultados del CV.

### 1.3 Qué demuestra (para quién)
| Audiencia | Lo que ve |
|---|---|
| Recruiter | Una frase del CV con números reales y un repo que se levanta con un solo comando |
| Entrevistador técnico | Decisiones justificadas: por qué Flink y no Spark, por qué eventos enriquecidos y no un lookup, por qué ONNX, cómo se midió |
| Tú | Dominio práctico de streaming con estado, presupuestos de latencia y MLOps online |

### 1.4 Métricas de éxito
| Tipo | Métrica | Meta | Nota |
|---|---|---|---|
| Latencia | p50 / **p95** / p99 de punta a punta (produce → decisión publicada) | p95 ≤ 80 ms | Se reporta con el desglose por tramo |
| Throughput | Eventos/s sostenidos sin que el p95 se dispare ni crezca el lag | 20k ev/s (`⏳ 16GB`); en 8 GB, lo que se mida | La cifra honesta va al CV |
| Modelo | PR-AUC; recall con un FPR fijo (p. ej., 1%) | Mejor que el baseline de reglas | El accuracy no sirve con clases desbalanceadas |
| Negocio (simulado) | Fraude detectado (monto) vs. fricción (% de `REVIEW` legítimos) | Se reporta el trade-off | Umbrales configurables |
| Resiliencia | Tiempo de recuperación tras matar el TaskManager; eventos perdidos o duplicados | 0 perdidos; duplicados cuantificados | Exactly-once vs at-least-once |
| Costo | Gasto en dinero | **$0** | Todo local |

### 1.5 Alcance
**Dentro:** todo lo anterior, Docker Compose con perfiles `lite` (8 GB) y `full` (`⏳ 16GB`), y una variante B de serving con **Triton** para comparar con ONNX in-process.
**Fuera:** datos reales de tarjetas (solo sintéticos, sin PII), Kubernetes, cloud, UI de analistas (se usa Grafana más la tabla de Postgres) y reentrenamiento online automático (queda documentado como una mejora).

### 1.6 Frase del CV (plantilla; se llena con números medidos)
> **Real-time Fraud Detection Platform** · Kafka · Flink SQL · Redis · XGBoost/ONNX · Grafana
> Streaming fraud scoring pipeline sustaining **{X}k events/s at p95 {Y} ms end-to-end** on a single 8 GB laptop; Flink windowed velocity features served via Redis, XGBoost served with ONNX Runtime ({Z}× faster than native), PyTorch challenger in shadow mode, exactly-once recovery in {T} s after TaskManager failure.

Versión corta: *"Kafka + Flink fraud scoring: {X}k ev/s, p95 {Y} ms, measured end-to-end."*

### 1.7 Narrativa para entrevista (30 segundos)
"Construí un detector de fraude en streaming. El reto era calcular features de comportamiento reciente y decidir en menos de 80 ms al p95. Elegí Flink sobre Spark porque procesa evento por evento. Para evitar una condición de carrera entre las features y el scoring, emito eventos ya enriquecidos. Serví XGBoost con ONNX Runtime y medí la latencia de punta a punta con percentiles, corrigiendo el sesgo de *coordinated omission*. En mi laptop llegué a {X} ev/s con p95 de {Y} ms; el cuello de botella fue {Z} y lo mejoraría con {W}."

### 1.8 Aporte al README
Este módulo se convierte en las secciones **Overview**, **The Problem**, **What This Demonstrates** y **Results** (esta última con placeholders hasta medir).

---

## 2. Arquitectura macro y flujo de datos

### 2.1 Vista general
```
                     ┌──────────────── CAMINO CRÍTICO (p95 ≤ 80 ms) ────────────────┐
┌───────────┐ payments  ┌──────────────┐ payments-enriched ┌──────────────┐ decisions
│ Generador │──────────►│  Flink SQL   │──────────────────►│    Scorer    │──────────►
│ (Python)  │ key=user  │ OVER windows │ evento + features │ XGBoost/ONNX │ APPROVE /
└─────┬─────┘ 12 part.  └──────────────┘                   └──────┬───────┘ REVIEW / BLOCK
      │             └─────────────────────────────────────────────│──────────────────┘
      │ labels (verdad de fraude, aparte)                         │ HGET perfil (pipeline)
      ▼                                                           ▼
┌───────────┐                                         ┌──────────────────────┐
│  labels   │                                         │ Redis (online store) │◄── Profile Builder
└─────┬─────┘                                         │ perfil + velocity    │    (batch: perfil 30d)
      │                                               └──────────▲───────────┘
      │          ┌──────────── FUERA DEL CAMINO CRÍTICO ─────────┼─────────────┐
      │          │ Feature Writer: payments-enriched ────────────┘ (último estado)
      │          │ Shadow Scorer (PyTorch → ONNX) ──► shadow-scores
      │          │ Explainer: decisions[REVIEW|BLOCK] → SHAP → LLM Gateway → explanations
      └─────────►│ Auditor: decisions + labels + shadow + explicaciones ──► PostgreSQL
                 │ Latency Collector: decisions → histogramas → Prometheus → Grafana
                 └─────────────────────────────────────────────────────────────┘
   MLflow: tracking + model registry (champion/challenger), para entrenar y desplegar
```

### 2.2 Dos caminos, dos reglas
| Camino | Qué incluye | Regla |
|---|---|---|
| **Crítico** | Generador → Kafka → Flink → Kafka → Scorer (+ lectura de Redis) → `decisions` | Todo lo que se agregue aquí debe justificar su costo en milisegundos. Nada de llamadas a un LLM, escrituras síncronas a Postgres ni saltos de red innecesarios |
| **Asíncrono** | Shadow, explicador, auditor, writer de features, colector de latencias | Puede ir lento o caerse **sin afectar** las decisiones. Cada uno es su propio consumer group |

### 2.3 Decisiones de diseño clave (ADRs resumidos)

**ADR-1 · Eventos enriquecidos, no consultar en Redis features recién escritas.**
Si Flink escribiera en Redis y el scorer consumiera `payments` en paralelo, el scorer podría leer features que **aún no incluyen el pago actual**. Eso es una carrera de datos y genera *train/serve skew*. Por eso Flink emite `payments-enriched`, con features que **ya incluyen el evento actual**, y el scorer consume de ahí.

**ADR-2 · Redis en el camino crítico solo para el *perfil*.**
Hay dos tipos de features:
- **Velocity** (`f_cnt_10m`, `f_sum_10m`, `f_distinct_countries_1h`…): cambian con cada evento. Las calcula Flink y viajan **dentro del evento**.
- **Perfil** (`avg_amount_30d`, `home_country`, `account_age_days`, `known_devices`): cambian lento. Las precalcula un **Profile Builder** batch y las guarda en Redis. El scorer las lee con `HGET` en pipeline: **una ida por micro-lote**.

Es el patrón híbrido que se usa en la industria: streaming para lo reciente y batch para lo estable. Además, Redis guarda el último estado de velocity (vía el Feature Writer) para consultas, para el explicador y para P2.

**ADR-3 · Ventanas `OVER`, no tumbling windows.**
Una tumbling window emite **al cerrarse**, lo que sumaría hasta 10 minutos de latencia. En cambio, `OVER (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW)` emite **una fila por evento** con el agregado al día. Es justo lo que pide el enriquecimiento por evento.

**ADR-4 · En el camino crítico, at-least-once con idempotencia en lugar de exactly-once.**
Con el sink transaccional (exactly-once) de Flink hacia Kafka, los consumers `read_committed` solo ven los datos **cuando se completa el checkpoint**. Eso suma una latencia del orden del intervalo de checkpoint (≥ 1 s) y rompe el p95 de 80 ms. Por eso:
- En el camino crítico se usa `AT_LEAST_ONCE` con **idempotencia**: la clave es `payment_id`, y el auditor y los consumers deduplican.
- El modo `EXACTLY_ONCE` se mide como experimento aparte, para **cuantificar el trade-off** entre latencia y garantías. Es material de entrevista de primer nivel.

**ADR-5 · La verdad de fraude no viaja dentro del evento.**
El generador publica las etiquetas en un topic aparte, `labels`. Si viajaran en `payments`, se filtrarían hacia las features (*label leakage*). Solo las usan el auditor y el entrenamiento.

**ADR-6 · El shadow tiene su propio consumer group.**
El challenger lee `payments-enriched` en un grupo separado y publica en `shadow-scores`, así que su impacto en la latencia del champion es **cero**. El auditor une ambos resultados por `payment_id`.

**ADR-7 · Serialización JSON en v1.**
Es simple con Flink SQL y no necesita Schema Registry, que con 8 GB sería un contenedor más consumiendo RAM. Avro con Schema Registry queda como alternativa documentada: payloads más pequeños y contratos de esquema, a cambio de un servicio extra.

### 2.4 Topics de Kafka
| Topic | Clave | Particiones | Productor → Consumidores | Retención |
|---|---|---|---|---|
| `payments` | `user_id` | 12 | Generador → Flink | 1 h |
| `payments-enriched` | `user_id` | 12 | Flink → Scorer, Shadow, Feature Writer | 1 h |
| `decisions` | `payment_id` | 12 | Scorer → Explainer, Auditor, Latency Collector | 24 h |
| `shadow-scores` | `payment_id` | 6 | Shadow → Auditor | 24 h |
| `labels` | `payment_id` | 6 | Generador → Auditor | 24 h |
| `explanations` | `payment_id` | 3 | Explainer → Auditor | 24 h |
| `dlq` | — | 1 | Cualquiera (eventos inválidos) | 7 d |

Se particiona por `user_id` porque **todas las ventanas son por usuario**. Así, los pagos de un mismo usuario llegan siempre a la misma partición: se conserva el orden y el estado de Flink queda local. Con 12 particiones, el scorer puede escalar hasta 12 consumers en paralelo.

### 2.5 Contratos de eventos (v1)
```jsonc
// payments
{ "payment_id": "uuid", "user_id": "u_000123", "card_id": "c_…", "amount": 42.50,
  "currency": "USD", "merchant_id": "m_…", "mcc": "5411", "country": "CO",
  "device_id": "d_…", "ip_country": "CO", "channel": "web",
  "event_time": "2026-10-05T14:03:22.123Z",   // tiempo de negocio (watermarks)
  "t_scheduled_ns": 0, "t_sent_ns": 0 }       // medición (ver §5)

// payments-enriched = payments + velocity features
{ …payments, "f_cnt_10m": 3, "f_sum_10m": 120.0, "f_cnt_1h": 5,
  "f_distinct_countries_1h": 2, "f_distinct_devices_24h": 1,
  "f_secs_since_last": 41.2, "f_small_tx_cnt_10m": 0, "t_flink_out_ns": 0 }

// decisions
{ "payment_id": "…", "user_id": "…", "decision": "REVIEW", "score": 0.73,
  "model": "xgb-champion", "model_version": "7", "threshold_set": "v1",
  "t_scheduled_ns": 0, "t_decided_ns": 0 }
```

### 2.6 El viaje de un pago
1. El generador **agenda** el pago en `t_scheduled` según la tasa objetivo, lo envía y marca `t_sent`.
2. Kafka lo escribe en la partición `hash(user_id) % 12`.
3. Flink lo lee, actualiza el estado del usuario en RocksDB, calcula las ventanas `OVER` y emite el evento enriquecido con `t_flink_out`.
4. El scorer hace poll de un micro-lote (hasta N mensajes o M ms), trae los perfiles de Redis en **una** ida en pipeline, arma la matriz de features e infiere **por lote** con ONNX Runtime. Luego aplica los umbrales y publica en `decisions` con `t_decided`.
5. Los consumers asíncronos reaccionan: el auditor persiste en Postgres, el explicador atiende los `REVIEW` y `BLOCK`, el shadow puntúa y el colector registra `t_decided − t_scheduled`.
6. El contexto de OTel viaja en los **headers** de Kafka, así que Grafana/Tempo puede mostrar el desglose por tramo de cualquier pago.

### 2.7 Variante B de serving (para comparar)
En esta variante, el scorer llama a **Triton** por gRPC (backend FIL para XGBoost, u ONNX) en lugar de usar ONNX in-process. Se comparan p95 y throughput: in-process evita la ida por red, mientras que Triton aporta dynamic batching y aislamiento. Va en un perfil de Compose aparte, `serving-triton`.

### 2.8 Aporte al README
Este módulo alimenta las secciones **Architecture** (con el diagrama), **Design Decisions** (los ADRs, explicados para quien no conoce streaming) y **Data Contracts**.

---

## 3. Componentes

> Todos siguen el mismo patrón, que es el mismo del README: **🧠 Concepto** (qué es y por qué existe) → **⚙️ Cómo funciona aquí** → **🔧 Detalle técnico** → **🔁 Alternativas**.
> ⚠️ Los parámetros son valores iniciales. Se ajustan midiendo (§5) y se verifican contra la versión exacta de cada herramienta al implementar.

### 3.1 Generador de carga (`generator/`)
**🧠 Concepto.** Para medir de forma creíble se necesita una carga **controlada y reproducible**, con patrones de fraude conocidos. Los datos reales de tarjetas no se pueden usar (PII y regulación), así que se simulan.
**⚙️ Cómo funciona aquí.** Simula una población de usuarios con hábitos propios: país, dispositivos, montos típicos y una frecuencia de actividad con distribución Zipf (pocos usuarios muy activos). Sobre esa base inyecta escenarios de fraude etiquetados:

| Escenario | Señal que debería detectarse |
|---|---|
| Ráfaga | Muchos pagos en pocos minutos |
| Salto geográfico | Dos países distintos en poco tiempo |
| Card testing | Varios montos pequeños seguidos |
| Monto atípico | Un monto muy por encima del promedio del usuario |
| Account takeover | Dispositivo nuevo + país nuevo + monto alto |

**🔧 Detalle técnico.**
- Python + `confluent-kafka` (cliente en C) + `orjson`. Corre en **varios procesos** para esquivar el GIL; cada uno atiende un subconjunto de usuarios.
- **Carga en lazo abierto (open-loop):** cada evento tiene una hora agendada (`t_scheduled`) según la tasa objetivo, y se envía en ese momento **sin esperar** al anterior. Así se evita el sesgo de *coordinated omission* (ver §5).
- Configuración del productor: `linger.ms=1`, `compression.type=lz4`, `acks=1`. Con un solo broker, `all` equivale a `1`; se documenta igualmente.
- Semilla fija y parámetros por CLI: `--rate 5000 --duration 300 --users 100000 --fraud-rate 0.008 --scenario-mix default`.
- Modo `--offline` que escribe Parquet: es el **histórico** del que salen el entrenamiento y el Profile Builder.

**🔁 Alternativas.** Generador en Go (más throughput por proceso, y reutiliza tu stack). Herramientas de carga como k6 con la extensión de Kafka (xk6-kafka). Datasets públicos como IEEE-CIS o PaySim (pero no son streaming ni se controla la tasa).

### 3.2 Kafka (`infra/kafka`)
**🧠 Concepto.** Es un log distribuido y persistente. Desacopla productores de consumidores, permite reprocesar y paraleliza por particiones. **KRaft** elimina ZooKeeper: un contenedor menos.
**⚙️ Cómo funciona aquí.** Un broker en modo combinado (broker + controller). Un contenedor `kafka-init` crea los topics de §2.4 con su configuración.
**🔧 Detalle técnico.**
- Imagen oficial `apache/kafka`, con heap de 512 MB a 1 GB en el perfil `lite`.
- `auto.create.topics.enable=false`.
- `message.timestamp.type=LogAppendTime` en `payments` y `decisions`, para que el broker marque el momento de llegada y sirva para medir.
- Retenciones cortas, para que los datos no llenen el disco.

**🔁 Alternativas.** **Redpanda**: compatible con la API de Kafka, en C++, sin JVM y más ligero en RAM; buen plan B si 8 GB no alcanzan. 3 brokers con réplicas (`⏳ 16GB`). Kafka UI solo en el perfil `full`.

### 3.3 Job de features en Flink (`flink/`)
**🧠 Concepto.** Es procesamiento con **estado** evento por evento. Flink recuerda la historia reciente de cada usuario (keyed state en RocksDB), recalcula los agregados con cada pago y garantiza la recuperación vía checkpoints.
**⚙️ Cómo funciona aquí.** Lee `payments`, particiona por `user_id`, calcula las features de velocity incluyendo el evento actual y emite `payments-enriched`.

**🔧 Detalle técnico (las perillas que más afectan el p95):**

| Perilla | Valor inicial | Por qué importa |
|---|---|---|
| `execution.buffer-timeout` | 1–5 ms (por defecto 100 ms) | **Crítico.** Flink acumula registros en buffers de red hasta llenarlos o hasta que vence este timeout. El valor por defecto puede sumar ~100 ms por salto |
| `table.exec.mini-batch.enabled` | `false` | El mini-batch sube el throughput a costa de latencia |
| Tiempo de la ventana `OVER` | **processing time** en el camino crítico; event time como experimento | Con event time, la fila se emite cuando pasa el watermark, lo que suma el *out-of-orderness* permitido más el intervalo del watermark. Como el generador escribe en orden por usuario y Kafka preserva el orden por partición, processing time es seguro aquí. El costo (resultados menos deterministas al reprocesar) se documenta |
| State backend | RocksDB con checkpoints incrementales cada 10 s | Estado acotado en RAM; recuperación rápida |
| Memoria del TaskManager | ~1.5 GB (`lite`) | Presupuesto de §8 |
| Paralelismo | 4 (`lite`) / 12 (`full` `⏳ 16GB`) | ≤ número de particiones |

**🚩 Riesgo técnico a validar con un *spike* (S1).** En Flink SQL, **todas las agregaciones `OVER` de un mismo `SELECT` deben usar la misma definición de ventana.** No se pueden calcular 10 m, 1 h y 24 h en una sola consulta. Opciones:

| Opción | Cómo | Pros | Contras |
|---|---|---|---|
| **A (por defecto)** | SQL con **una** ventana `OVER` de 10 m para las features calientes. Las de 1 h y 24 h se calculan en otra consulta y van a Redis vía el Feature Writer | Todo en SQL, sencillo y rápido | Las features largas pueden estar desfasadas por uno o dos eventos (*skew* pequeño, que se documenta) |
| **B** | PyFlink DataStream con un `KeyedProcessFunction` que mantiene un buffer de 24 h por usuario y calcula todas las features en una pasada, en **modo thread** | Todas las ventanas exactas, en un solo operador | Python en el operador. El modo thread reduce mucho el costo, pero hay que medirlo |
| **C** | El mismo `KeyedProcessFunction` en Java | La menor latencia posible | Un lenguaje extra en el repo |

El plan es implementar A, medir B y documentar C. También hay que verificar que se puede usar `COUNT(DISTINCT)` dentro de `OVER` en la versión de Flink que se use; si no, se reemplaza con una UDAF.

**🔁 Alternativas.** Spark Structured Streaming (micro-lotes; se compara en el curso C2). Bytewax o Faust (Python nativo). Kafka Streams (JVM, sin clúster aparte).

### 3.4 Profile Builder (`profile_builder/`)
**🧠 Concepto.** Las features estables, como el comportamiento de 30 días, no necesitan streaming. Calcularlas en batch es más barato y simple. Es la mitad "batch" del patrón híbrido (ADR-2).
**⚙️ Cómo funciona aquí.** Lee el histórico en Parquet, calcula el perfil por usuario, lo escribe en Redis y genera el **mismo** dataset que usa el entrenamiento. La misma función alimenta ambos lados, así que no hay *train/serve skew*.
**🔧 Detalle técnico.** Polars o DuckDB. Escribe en Redis con pipelines de 1.000 claves. Corre al levantar el stack (`make profiles`) y bajo demanda. Las features: `avg_amount_30d`, `std_amount_30d`, `home_country`, `n_known_devices`, `account_age_days`, `usual_hour_bucket`.
**🔁 Alternativas.** Feast con Redis como online store (agrega gobierno y registry, pero también otra pieza). Para la versión `full`, un job batch en Flink.

### 3.5 Redis (`infra/redis`)
**🧠 Concepto.** Es un *online store*: lecturas en microsegundos, en memoria, de la **última** versión de las features de cada entidad.
**⚙️ Cómo funciona aquí.**

| Clave | Tipo | Contenido | TTL |
|---|---|---|---|
| `profile:{user_id}` | Hash | Perfil de 30 días (camino crítico) | Sin TTL; lo reconstruye el Profile Builder |
| `vel:{user_id}` | Hash | Último estado de velocity (consultas, explicador, P2) | 24 h |
| `vel_long:{user_id}` | Hash | Features de 1 h y 24 h (opción A de §3.3) | 24 h |

**🔧 Detalle técnico.** `maxmemory 256mb` con `noeviction` (100k perfiles ocupan ~20–30 MB). Persistencia (AOF/RDB) **apagada**, porque todo se puede reconstruir. Cliente `redis-py` con `hiredis`. Lecturas con **una ida en pipeline por micro-lote**.
**🔁 Alternativas.** Dragonfly o Valkey (compatibles con Redis). Feast con Redis. Tablas de estado consultables desde Flink (complejo).

### 3.6 Scorer — champion (`scorer/`)
**🧠 Concepto.** Es la etapa de decisión. Convierte features en una probabilidad y la probabilidad en una acción de negocio con umbrales.
**⚙️ Cómo funciona aquí.** Consume `payments-enriched` en micro-lotes, completa el perfil desde Redis, infiere **por lote** con ONNX Runtime, aplica los umbrales (`APPROVE` < t1 ≤ `REVIEW` < t2 ≤ `BLOCK`) y publica en `decisions`.

**🔧 Detalle técnico.**
- Batching: `consumer.consume(num_messages=500, timeout=0.005)`. El lote se cierra con 500 mensajes o a los 5 ms, lo que llegue primero. Es el balance entre throughput y latencia, y se ajusta en §5.
- Inferencia: `onnxruntime.InferenceSession` con `intra_op_num_threads=1` por proceso. Se escala con procesos (1 por cada N particiones), no con threads.
- Entrega: el productor publica de forma asíncrona con callback, y los offsets se confirman **después** del `flush` del lote (at-least-once, ADR-4).
- Modelo: al arrancar, carga la versión con alias `champion` desde el **registry de MLflow** y revisa el alias cada 30 s para hacer hot-reload sin reiniciar.
- Umbrales en `thresholds.yaml`, versionados (`threshold_set` va en cada decisión).
- Métricas de Prometheus (histogramas por etapa: poll, Redis, inferencia, produce) y spans de OTel muestreados al 1%, para que la observabilidad no se coma el presupuesto.

**🔁 Alternativas.** XGBoost nativo (base del benchmark). Triton por gRPC (Variante B, §2.7). Scorer en Go con ONNX Runtime (si Python es el cuello de botella; buena "mejora futura" para la entrevista).

### 3.7 Admin & Scoring API (`api/`)
**🧠 Concepto.** No todo el tráfico llega por Kafka. Las demos, P2 y los analistas necesitan una puerta HTTP.
**⚙️ Cómo funciona aquí.** Un servicio FastAPI **separado** del scorer, para no meter un servidor HTTP en el lazo de consumo:
- `POST /score`: puntúa un pago puntual.
- `GET /users/{id}/features`: devuelve el perfil y la velocity desde Redis.
- `GET /decisions/{payment_id}`: devuelve la decisión desde Postgres, con su explicación.
- `GET /model`: devuelve la versión del champion.
- `/health` y `/metrics`.

**🔧 Detalle técnico.** Uvicorn con 1 worker en `lite`. Usa la misma librería de features e inferencia que el scorer (paquete compartido `fraudcore/`).
**🔁 Alternativas.** El camino gRPC de máquina a máquina se implementa aparte, en §14.

### 3.8 Shadow Scorer — challenger (`scorer/` en modo shadow)
**🧠 Concepto.** *Shadow mode* evalúa un modelo nuevo con tráfico **real**, sin que sus decisiones tengan efecto. Es la forma segura de promover modelos.
**⚙️ Cómo funciona aquí.** Usa el mismo binario del scorer con `--mode shadow`: carga el alias `challenger` (un MLP de PyTorch exportado a ONNX), corre en su propio consumer group y publica en `shadow-scores`. El auditor compara champion contra challenger con las etiquetas reales.
**🔧 Detalle técnico.** Como corre en otro proceso y otro grupo, el p95 del champion no cambia (se verifica en §5). La promoción se hace cambiando el alias en MLflow.
**🔁 Alternativas.** Canary (un porcentaje del tráfico **sí** usa al challenger) o A/B. Se documentan como el siguiente paso tras el shadow.

### 3.9 Feature Writer (`feature_writer/`)
**🧠 Concepto.** Publicar el último estado de cada usuario para que otros servicios lo consulten, **sin** que eso esté en el camino crítico.
**⚙️ Cómo funciona aquí.** Consume `payments-enriched` (y la consulta de ventanas largas de la opción A) y hace `HSET` en Redis en pipelines.
**🔧 Detalle técnico.** Lotes de 500 o cada 50 ms. Idempotente, porque el último valor gana.
**🔁 Alternativas.** Un sink de Redis dentro de Flink (los conectores de Redis para Flink no son oficiales; se evita depender de uno sin mantenimiento).

### 3.10 Explainer (`explainer/`)
**🧠 Concepto.** Un analista no puede actuar sobre un "score 0.87". Necesita **por qué**. SHAP da contribuciones exactas por feature para modelos de árboles, y el LLM las convierte en lenguaje natural. Es el patrón **"XGBoost decide y el LLM redacta"**.
**⚙️ Cómo funciona aquí.** Consume `decisions` y filtra `REVIEW` y `BLOCK`. Recupera las features (del evento enriquecido guardado o de Redis), calcula SHAP con el booster **nativo** de XGBoost (de la misma versión del registry que el ONNX), arma un prompt con el top-5 de contribuciones y llama al **LLM Gateway (`llm-gateway`, Python)**. El resultado va a `explanations`.
**🔧 Detalle técnico.**
- SHAP con `TreeExplainer`, porque el grafo ONNX no sirve para SHAP.
- Gateway con API compatible con OpenAI; proveedor `mock` en tests y `ollama` (Gemma 3 1B o Qwen3 1.7B) en local.
- **Backpressure:** concurrencia de 1 o 2. Si el LLM va más lento que el flujo de casos, **muestrea** y marca `explanation_status=skipped`, en vez de acumular lag infinito.
- **Fallback:** si el gateway falla (su circuit breaker abre), usa una plantilla determinista a partir de SHAP.

**🔁 Alternativas.** Solo plantillas (sin LLM). Un LLM vía API (Haiku); queda documentado, pero **no** se usa en P1 por la regla de gasto cero.

### 3.11 Auditor + PostgreSQL (`auditor/`, `infra/postgres`)
**🧠 Concepto.** Es la memoria de largo plazo del sistema: qué se decidió, con qué modelo y si fue fraude de verdad. Sin esto no se pueden evaluar los modelos en producción ni reentrenar.
**⚙️ Cómo funciona aquí.** Consume `decisions`, `labels`, `shadow-scores` y `explanations` y los persiste.

| Tabla / vista | Para qué |
|---|---|
| `decisions`, `labels`, `shadow_scores`, `explanations` | Los datos crudos, uno por `payment_id` |
| `v_confusion_by_model` | Matriz de confusión del champion y del challenger |
| `v_business_tradeoff` | Monto de fraude detectado vs fricción |
| `v_feedback_dataset` | Dataset para reentrenar (features + etiqueta real) |

**🔧 Detalle técnico.** Inserciones por lote con `COPY` o `executemany`. `ON CONFLICT (payment_id) DO NOTHING` (idempotencia, ADR-4). `shared_buffers=128MB` en `lite`.
**🔁 Alternativas.** ClickHouse o DuckDB para la analítica. Un sink JDBC desde Flink.

### 3.12 Latency Collector (`latency/`)
**🧠 Concepto.** Es la pieza que hace **creíble** la frase del CV: mide percentiles reales de punta a punta, no promedios.
**⚙️ Cómo funciona aquí.** Consume `decisions` y calcula la latencia de punta a punta como el `LogAppendTime` del broker en `decisions` menos `t_scheduled`, además del desglose por tramo con los timestamps intermedios. Todo corre en un solo host, así que hay un único reloj.
**🔧 Detalle técnico.**
- `HdrHistogram` por corrida y por escalón de carga.
- Exporta un histograma de Prometheus (con buckets afinados alrededor de 10–150 ms) para Grafana.
- Guarda `results/<run_id>.parquet` y genera la **tabla de resultados del README**.

**🔁 Alternativas.** Calcular la latencia solo con las métricas de Prometheus (menos precisa en las colas).

### 3.13 MLflow (`infra/mlflow`)
**🧠 Concepto.** Tracking de experimentos y **model registry** con aliases (`champion`, `challenger`). Los servicios piden "el champion", no una ruta de archivo.
**⚙️ Cómo funciona aquí.** El entrenamiento registra parámetros, métricas (PR-AUC, recall con FPR fijo), el booster nativo, el `.onnx` y la firma de features. El scorer y el explainer leen la versión del alias.
**🔧 Detalle técnico.** Backend en SQLite con artefactos locales en `lite` (Postgres como backend en `full`). Solo se levanta para entrenar o promover. El scorer **cachea** el modelo en disco, así que no depende de que MLflow esté arriba durante el benchmark.
**🔁 Alternativas.** W&B (cloud). Registry propio en S3 o MinIO.

### 3.14 Observabilidad (`infra/observability`) — detalle en §6
Prometheus (métricas), Grafana (dashboards como código), OTel Collector y Tempo (trazas, perfil `full`). Exporters de Kafka y Redis, y métricas nativas de Flink.

### 3.15 LLM Gateway (`llm-gateway`, proyecto P0 en Python)
Es el gateway propio, en Python y construido desde cero (ver `../llm-gateway/PLAN.md`). Se incluye como contenedor en el Compose (perfil `explain`). P1 solo lo usa desde el Explainer, con el alias `fast` (`mock` en tests, `ollama` en local). Si el gateway no está, el Explainer pasa a la plantilla basada en SHAP.

### 3.16 Aporte al README
Cada componente se convierte en una subsección de **Components**, con la misma estructura: Concepto → Cómo funciona → *Technical details* al final.

---

## 4. Modelo

### 4.1 Por qué XGBoost (concepto)
- **Datos tabulares:** el gradient boosting sobre árboles sigue siendo el estado del arte práctico en datos tabulares. Captura interacciones no lineales (monto alto **y** país nuevo) sin ingeniería manual.
- **Latencia:** infiere en ~1–5 ms en CPU, y en lote lo hace todavía más barato por evento.
- **Explicable:** SHAP para árboles es exacto y rápido (`TreeExplainer`).
- **Por qué no un LLM:** cientos de ms y costo por token para una decisión sobre números. El LLM solo **redacta** la explicación (§3.10).
- **Por qué no deep learning como modelo principal:** en tablas pequeñas rara vez supera a los árboles. Por eso es el **challenger**, y el proyecto lo demuestra con datos.

### 4.2 Datos de entrenamiento
| Aspecto | Decisión | Por qué |
|---|---|---|
| Fuente | `generator --offline`: ~60 días simulados, ~3–5 M pagos en Parquet | Cabe en 8 GB con Polars lazy |
| Tasa de fraude | ~0.8% | Realista: el desbalance extremo es el problema central |
| Split | **Temporal**: días 1–40 train, 41–50 validación, 51–60 test | Un split aleatorio filtra el futuro (*leakage*); en producción siempre se predice el futuro |
| Usuarios | Se reporta además el rendimiento en usuarios **no vistos** | Evita que el modelo "memorice" usuarios |
| Retraso de etiquetas | Opcional: las etiquetas llegan con N días de retraso (como un chargeback) | Hace realista el lazo de feedback de §4.10 |

### 4.3 Features
| Grupo | Feature | Fuente |
|---|---|---|
| Velocity | `f_cnt_10m`, `f_sum_10m`, `f_small_tx_cnt_10m`, `f_secs_since_last` | Flink (evento) |
| Velocity larga | `f_cnt_1h`, `f_distinct_countries_1h`, `f_distinct_devices_24h` | Flink (§3.3, opción A o B) |
| Perfil | `avg_amount_30d`, `std_amount_30d`, `home_country`, `n_known_devices`, `account_age_days`, `usual_hour_bucket` | Profile Builder → Redis |
| Derivadas | `amount_to_avg_ratio`, `amount_zscore`, `is_foreign = country ≠ home_country`, `is_new_device`, `ip_country_mismatch`, `hour_deviation` | Calculadas en el scorer (`fraudcore`) |
| Pago | `amount`, `mcc` (target encoding o frecuencia), `channel` | Evento |

**🛡️ Paridad de features (el punto más importante de MLOps en este proyecto).** Las features se calculan **dos veces**: offline en Python o Polars para entrenar, y online en Flink SQL. Si difieren, el modelo se degrada en silencio (*train/serve skew*). Por eso:
1. Hay una sola especificación en `fraudcore/features.py`, con definiciones, tipos y orden de columnas.
2. **Test de paridad:** se reproduce una muestra del histórico a través de Flink y se compara con el cálculo offline, con tolerancia numérica. El test corre en la CI y **falla** si hay diferencias.
3. Las derivadas solo existen en `fraudcore`, compartido por el entrenamiento, el scorer y la API.

### 4.4 Baseline de reglas
Antes del ML hay un motor de reglas simple (p. ej., `f_cnt_10m > 5`, o `is_foreign` **y** `amount_to_avg_ratio > 3`). Así se demuestra **cuánto aporta el modelo** frente a lo que haría un equipo sin ML. Es la primera fila de la tabla de resultados.

### 4.5 Entrenamiento del champion (XGBoost)
- `tree_method="hist"` en CPU, `scale_pos_weight` según el desbalance, y early stopping sobre `aucpr` en validación.
- Búsqueda con **Optuna** acotada (~30–50 trials, con timeout), en CPU.
- **Restricciones monótonas** opcionales (p. ej., el riesgo no baja cuando sube `amount_to_avg_ratio`): mejoran la explicabilidad y la robustez. Se mide si cuestan rendimiento.
- Todo queda en **MLflow**: parámetros, métricas, curva PR, importancia SHAP global, booster nativo, `.onnx`, firma de features y hash del dataset.

### 4.6 Umbrales: de probabilidad a decisión
Un score no es una decisión. Se eligen dos umbrales (`t1`, `t2`) **minimizando el costo esperado** en validación:

| Resultado | Costo simulado |
|---|---|
| Fraude aprobado | Monto del pago |
| `REVIEW` | Costo fijo por revisión (tiempo del analista) |
| Legítimo bloqueado | Costo de fricción o pérdida del cliente |

Con la restricción de que la tasa de `REVIEW` sea ≤ X% (la capacidad del equipo de analistas). El resultado va a `thresholds.yaml` y se registra en MLflow. La curva costo vs umbral entra en el README. Es justo el tipo de razonamiento de negocio que buscan en una entrevista.

### 4.7 Exportación a ONNX y verificación
1. Conversión con `onnxmltools` (convertidor de XGBoost). Hummingbird queda como alternativa.
2. **Test de paridad numérica:** |pred_ONNX − pred_nativo| < 1e-5 en todo el test set. Si falla, no se registra.
3. **Benchmark** nativo vs ONNX con tamaños de lote 1, 32 y 500, con p50 y p99 por lote y por evento. Se registra en MLflow y alimenta la cifra "{Z}× faster" del CV.

### 4.8 Challenger (PyTorch)
- **MLP** sobre las mismas features: normalización **dentro** del grafo del modelo (para que el escalado viaje con él y no haya skew), con embeddings para `mcc` y `country`.
- Entrenado en tu GPU de 4 GB (o en CPU; es pequeño). Pérdida ponderada o focal por el desbalance.
- Exportado con `torch.onnx.export`, con el mismo test de paridad que el champion.
- Variante opcional: **autoencoder** de anomalías (no supervisado), para detectar patrones de fraude **nuevos** sin etiquetas.

### 4.9 Evaluación offline
| Métrica | Por qué |
|---|---|
| **PR-AUC** | La métrica honesta para clases desbalanceadas (ROC-AUC luce bien aunque el modelo sea malo) |
| **Recall con FPR = 1%** | La pregunta de negocio: "¿cuánto fraude atrapo molestando a 1 de cada 100 clientes legítimos?" |
| **Recall por escenario** | ¿Qué tipos de fraude detecta y cuáles se le escapan? (ráfaga, salto geográfico, card testing…) |
| **Costo esperado** | El de §4.6, con los umbrales elegidos |
| Usuarios no vistos | Generalización |
| Latencia de inferencia | Debe caber en el presupuesto (§5) |

Tabla final: **Reglas vs XGBoost nativo vs XGBoost en ONNX vs MLP challenger**.

### 4.10 Evaluación online y promoción
- El auditor une `decisions`, `shadow-scores` y `labels`, y las vistas `v_confusion_by_model` y `v_business_tradeoff` comparan los modelos con tráfico **real** del stack.
- **Criterio de promoción** (escrito antes de mirar los resultados): el challenger mejora el recall con FPR de 1% en ≥ X puntos sobre ≥ N eventos etiquetados, con latencia dentro del presupuesto.
- La promoción se hace con `make promote MODEL=challenger`, que mueve el alias en MLflow. El scorer lo recarga en caliente (§3.6), **sin reiniciar ni perder eventos**.

### 4.11 🎬 Demo de drift (la historia más fuerte del proyecto)
1. El stack corre estable y el champion detecta bien.
2. El generador activa un **patrón de fraude nuevo** a mitad de la corrida (`--inject-scenario new_pattern`).
3. Grafana muestra cómo cae el recall del champion y cómo crecen los fraudes aprobados en las vistas del auditor.
4. Se reentrena con `v_feedback_dataset`, se registra como challenger, el shadow confirma la mejora y se promueve.
5. Grafana muestra la recuperación.

Es el ciclo completo de MLOps online en vivo, y se graba como GIF para el README. Detección de drift opcional con ADWIN o Page-Hinkley (curso `09/40/04`).

### 4.12 Aporte al README
Secciones **The Model** (por qué XGBoost, features, paridad), **From Score to Decision** (umbrales por costo), **Model Lifecycle** (shadow, promoción, drift) y la tabla de evaluación offline.

---

## 5. Metodología de medición y pruebas de carga

### 5.1 Concepto: por qué percentiles
El promedio esconde lo que importa. Si 94 pagos tardan 20 ms y 6 tardan 2 s, el promedio da ~140 ms y no cuenta la historia: **6 de cada 100 clientes** esperaron 2 segundos. El **p95** responde a "¿cuánto espera el 95% de los clientes como máximo?", y el **p99** muestra la cola. Con miles de eventos por segundo, la cola **es** la experiencia de miles de personas.

### 5.2 Definición exacta de latencia
**Latencia de punta a punta** = `LogAppendTime` del broker al escribir en `decisions` − `t_scheduled` (la hora en que el pago **debía** salir).

| Tramo | Cálculo | Qué revela |
|---|---|---|
| `gen_lag` | `t_sent − t_scheduled` | Si el generador no da abasto. **Si crece, la corrida no es válida** |
| `kafka_in` | append en `payments` − `t_sent` | Productor y broker |
| `flink` | `t_flink_out − append en payments` | Ventanas, estado, buffers de red |
| `hop_mid` | `t_received (scorer) − t_flink_out` | Kafka intermedio y espera del poll |
| `scorer` | `t_decided − t_received` | Subtramos: espera del lote, Redis, inferencia, armado |
| `kafka_out` | append en `decisions` − `t_decided` | Produce y flush |

- **Reloj:** todo corre en el mismo host (los contenedores de Docker Desktop comparten la VM de WSL2), así que hay un único reloj y no hace falta sincronizar con NTP. El `LogAppendTime` tiene resolución de ms, suficiente para una meta de 80 ms.
- Los tramos se guardan **por evento** en el Latency Collector, y una muestra se envía como trazas de OTel para inspeccionar casos individuales.

### 5.3 *Coordinated omission* (el error clásico)
Si el generador espera a que un envío termine antes de mandar el siguiente, cuando el sistema se pone lento **también deja de enviar**. Los eventos que "debieron" sufrir la lentitud nunca existen, y el p95 sale artificialmente bueno.
**Solución:** carga en **lazo abierto** (§3.1). Cada evento se agenda con una tasa fija y se mide desde `t_scheduled`, no desde `t_sent`. **Regla de validez:** si el p99 de `gen_lag` supera 5 ms, el generador es el cuello de botella y la corrida se descarta (o se agregan procesos generadores).

### 5.4 Protocolo de carga por escalones
```
tasa (ev/s): 1k → 2k → 5k → 8k → 10k → 15k → 20k (→ más en ⏳16GB)
cada escalón: [ 60 s calentamiento, descartado ] [ 180 s medición ] [ drenar lag a 0 ]
cada perfil:  3 repeticiones completas → se reporta la mediana del p95 y su rango
```
**Throughput sostenido** = la mayor tasa en la que se cumplen **las cuatro** condiciones:
1. p95 ≤ 80 ms.
2. El lag de **todos** los consumer groups del camino crítico es estable (pendiente ≈ 0, no crece).
3. `gen_lag` es válido (§5.3).
4. 0 errores, y la DLQ sin crecimiento anómalo.

Se grafica el **"codo"** (p95 y p99 vs tasa), que muestra dónde se satura el sistema.

### 5.5 Condiciones de la corrida (higiene)
- Laptop **conectada a la corriente**, plan de energía de alto rendimiento y apps pesadas cerradas (el *thermal throttling* falsea las colas).
- `.wslconfig` con un límite de memoria explícito; se registra junto con la versión de Docker Desktop.
- Perfil `lite`: Grafana **apagada** durante la medición y encendida después para revisar (Prometheus sí sigue recolectando).
- **Manifiesto por corrida** (`results/<run_id>/manifest.json`): git SHA, perfil de Compose, configuración efectiva (buffer-timeout, lote, paralelismo…), versión del modelo, CPU, núcleos, RAM, disco, SO y versiones.

### 5.6 Matriz de experimentos
| ID | Experimento | Variable | Hipótesis / qué enseña |
|---|---|---|---|
| **E1** | Línea base | Carga por escalones (`lite`) | El throughput sostenido real del laptop: **la cifra del CV** |
| **E2** | Batching del scorer | Lote 1 / 50 / 500 × timeout 1 / 5 / 20 ms | La curva throughput ↔ latencia |
| **E3** | Buffer de red de Flink | `buffer-timeout` 100 (default) / 10 / 1 ms | Cuánto p95 cuesta el default (gráfica muy didáctica) |
| **E4** | Serving | XGBoost nativo / ONNX in-process / Triton (Variante B) | ONNX vs nativo; costo de la red vs dynamic batching |
| **E5** | Garantías | At-least-once vs exactly-once (checkpoint de 1 / 5 / 10 s) | Cuantifica el ADR-4 |
| **E6** | Tiempo de las ventanas | Processing time vs event time (out-of-orderness de 0.5 / 2 s) | El costo de los watermarks |
| **E7** | Spike S1 | Features opción A (SQL) vs B (PyFlink thread mode) | Si Python en el operador cabe en el presupuesto |
| **E8** | Shadow | Challenger apagado / encendido | Impacto ≈ 0 en el champion (lo valida el ADR-6) |
| **E9** | Costo de observar | Muestreo de OTel 0 / 1 / 100% | Cuánto cuesta medir |
| **E10** | Escalado | 1 / 2 / 4 procesos scorer; paralelismo de Flink 2 / 4 | Dónde se satura cada etapa |
| E1-full | Línea base completa | `full` con 20k+ ev/s | `⏳ 16GB` |

### 5.7 Cómo encontrar el cuello de botella
| Síntoma | Dónde mirar | Etapa culpable |
|---|---|---|
| Crece el lag del grupo de Flink en `payments` | Exporter de Kafka | Flink |
| `backPressuredTimeMsPerSecond` alto en la fuente de Flink | Métricas de Flink | Operador aguas abajo o sink |
| Crece el lag del grupo `scorer` en `payments-enriched` | Exporter de Kafka | Scorer |
| Sube `scorer` y su subtramo `redis` | Histogramas del scorer | Redis o red |
| CPU de un contenedor al 100% | Muestreo de `docker stats` (script ligero; cAdvisor solo en `full`) | Ese servicio |
| Crece `gen_lag` | Collector | El generador: corrida inválida |

### 5.8 Herramientas
- `bench/run.py`: orquesta un experimento completo (aplica la configuración, levanta perfiles, recorre los escalones, recolecta y escribe el manifiesto). Se llama con `make bench EXP=E1 PROFILE=lite REPS=3`.
- `bench/report.py`: genera la **tabla de resultados en Markdown** y las gráficas (codo p95/p99 vs tasa, barras apiladas por tramo, CDF de latencia, comparativas de E2–E10) en `docs/results/`.
- `bench/stats_sampler.py`: muestrea `docker stats` cada segundo durante la corrida.

### 5.9 Plantilla de la tabla de resultados (README)
| Perfil | Hardware | Tasa (ev/s) | p50 (ms) | p95 (ms) | p99 (ms) | Lag estable | Cuello de botella |
|---|---|---|---|---|---|---|---|
| lite | {CPU} · 8 GB · 4 GB VRAM | 5,000 | — | — | — | ✅ | — |
| lite | … | {máx. sostenido} | — | — | — | ✅ | {etapa} |
| full `⏳ 16GB` | … | 20,000 | — | — | — | — | — |

### 5.10 Reglas de honestidad
- Se publican **todas** las repeticiones, no la mejor.
- La frase del CV cita el **throughput sostenido** con p95 ≤ meta (§5.4) e incluye el hardware.
- Si no se llega a la meta, se documenta el cuello de botella, la evidencia y la mejora propuesta. Ese análisis vale más en una entrevista que un número inflado.

### 5.11 Aporte al README
Secciones **How We Measure** (definición, coordinated omission, protocolo), **Results** (tabla + gráficas) y **Bottleneck Analysis**.

---

## 6. Observabilidad

### 6.1 Concepto: tres preguntas, tres señales
| Pregunta | Señal | Herramienta |
|---|---|---|
| "¿Está sano? ¿Cuánto, qué tan rápido?" | **Métricas** (agregadas y baratas) | Prometheus + Grafana |
| "¿Por qué *este* pago tardó 300 ms?" | **Trazas** (un viaje de punta a punta) | OpenTelemetry + Tempo |
| "¿Qué pasó exactamente?" | **Logs** (eventos discretos) | JSON a stdout (+ Loki en `full`) |

La §5 mide para el **CV**. La observabilidad sirve para **operar**: detectar, diagnosticar y alertar mientras el sistema corre.

### 6.2 Stack por perfil
| Componente | `lite` (8 GB) | `full` (`⏳ 16GB`) |
|---|---|---|
| Prometheus (scrape cada 5 s, retención de 6 h) | ✅ | ✅ |
| Grafana | ✅ Bajo demanda (apagada durante el benchmark) | ✅ Siempre |
| kafka-exporter + redis_exporter | ✅ | ✅ |
| Reporter Prometheus de Flink (integrado) | ✅ | ✅ |
| OTel Collector + Tempo | ⚪ Opcional (se usa el desglose por evento de §5) | ✅ |
| Loki | ❌ (`docker logs`) | ✅ |
| cAdvisor | ❌ (`stats_sampler`) | ✅ |

### 6.3 Catálogo de métricas
| Capa | Métricas clave |
|---|---|
| **Negocio** | Tasa de `APPROVE`/`REVIEW`/`BLOCK`, monto bloqueado por minuto, tasa de revisión frente a la capacidad |
| **Modelo** | Distribución del score (histograma), **PSI** del score y de las features clave frente al entrenamiento, acuerdo champion↔challenger, recall online (desde Postgres cuando llegan etiquetas) |
| **Pipeline** | Eventos/s por topic, **lag por consumer group y partición**, `busyTimeMsPerSecond` y `backPressuredTimeMsPerSecond` de Flink, duración y fallas de checkpoints, tasa de la DLQ |
| **Serving** | Histograma de latencia de punta a punta, histogramas por tramo y subtramo (Redis, inferencia, produce), tamaño real del lote |
| **Explainer / LLM** | Llamadas/s, latencia, tasa de *fallback* a plantilla, tasa de `skipped`, cache hits del gateway |
| **Infra** | CPU y memoria por contenedor |

**🔧 Convenciones.**
- Nombres con unidad (`fraud_e2e_latency_seconds`, `fraud_stage_latency_seconds{stage="redis"}`).
- Buckets densos alrededor de la meta: `[.005, .01, .02, .03, .04, .05, .06, .07, .08, .09, .1, .15, .25, .5, 1, 2.5]`.
- ⚠️ **Nunca** usar `user_id` ni `payment_id` como labels: explota la cardinalidad y tumba Prometheus. Esos IDs van en las trazas y los logs.

### 6.4 Dashboards como código
Se versionan en `observability/grafana/dashboards/*.json` y se cargan con *provisioning*: `docker compose up` los trae listos, sin clics manuales.

| Dashboard | Pregunta que responde | Paneles principales |
|---|---|---|
| **D1 · Overview** | ¿Está sano? | Throughput, p50/p95/p99 de punta a punta, lag total, mezcla de decisiones, estado del SLO |
| **D2 · Latency Breakdown** | ¿Dónde se va el tiempo? | Barras apiladas por tramo, heatmap de latencia, tamaño de lote vs latencia |
| **D3 · Kafka & Flink** | ¿Qué pasa por dentro del streaming? | Lag por partición, backpressure por operador, checkpoints, records in/out |
| **D4 · Model & Business** | ¿El modelo sigue funcionando? | Distribución del score, PSI, champion vs challenger, recall online y trade-off de negocio (datasource **PostgreSQL** sobre las vistas del auditor) |
| **D5 · Explainer & LLM** | ¿El explicador da abasto? | Latencia del LLM, fallback, skipped, cache del gateway |

D4 es el que cuenta la **demo de drift** (§4.11): el PSI sube, el recall baja, se promueve el challenger y todo se recupera.

### 6.5 SLOs y alertas
**SLO principal (SLI como proporción):** "≥ 95% de las decisiones se publican en < 80 ms", medido en ventanas móviles. Es la definición del p95 escrita de forma que permite calcular **burn rate**.

| Alerta | Condición | Severidad |
|---|---|---|
| Latency SLO burn | Burn rate alto en dos ventanas (5 min **y** 1 h) | 🔴 |
| Lag creciente | Lag del camino crítico con pendiente positiva sostenida o > 30 s | 🔴 |
| Checkpoints fallando | ≥ 2 fallas seguidas en Flink | 🔴 |
| DLQ | Tasa > 0 sostenida | 🟠 |
| Anomalía de `BLOCK` | Tasa de bloqueo fuera de la banda esperada (indica un bug de datos o de modelo) | 🟠 |
| **Drift** | PSI del score o de una feature clave > 0.2 | 🟠 (dispara la demo §4.11) |
| Explainer degradado | Fallback o skipped > X% | 🟡 |

Las alertas se ven en la UI de Grafana (contact point local). Un webhook hacia afuera queda documentado como opción, sin activarse.

### 6.6 Trazas distribuidas
**🧠 Concepto.** Una traza sigue **un** pago a través de todos los servicios. El contexto (`traceparent`, estándar W3C) viaja en los **headers de Kafka**.

**⚙️ Cómo funciona aquí.**
1. El generador abre la traza y **muestrea el 1%** (head-based), marcando la decisión en el header.
2. Flink SQL **preserva el header**: el conector de Kafka de Flink permite leer y escribir `headers` como columna de metadatos. Flink no crea spans propios.
3. El scorer, el explainer y el auditor continúan la traza con sus spans.
4. Para el tramo de Flink, el scorer crea un **span sintético** con los timestamps registrados (append en `payments` → `t_flink_out`). Es pragmático y se documenta como tal.

**🔧 Detalle técnico.** SDK de OTel para Python con exportador OTLP hacia el Collector y de ahí a Tempo. En Grafana, *exemplars* conectan un punto del histograma de latencia con su traza: se hace clic en un pico y se abre el pago concreto. Hay que verificar al implementar que Flink SQL lee y escribe headers en la versión usada.

### 6.7 Logs
- JSON estructurado con `payment_id`, `trace_id`, `service` y `model_version`, para correlacionarlos con las trazas.
- `INFO` por defecto. El camino crítico **no** escribe un log por evento (solo errores y muestras), porque loguear a 20k ev/s es en sí un cuello de botella.
- Los datos son sintéticos, pero se mantiene la disciplina: **nunca** se loguean números de tarjeta (buena práctica para la entrevista).

### 6.8 Presupuesto de observabilidad (`lite`)
Prometheus ~300 MB (6 h de retención, cardinalidad controlada). Grafana ~150 MB, solo bajo demanda. Los exporters, ~30 MB cada uno. El impacto en el p95 se mide en E9 (§5.6).

### 6.9 Aporte al README
Sección **Observability** con capturas de D1, D2 y D4, la tabla de SLOs y alertas, y un ejemplo de traza de un pago lento. La sección incluye un GIF de la demo de drift.

---

## 7. Pruebas de falla y resiliencia

### 7.1 Concepto: caos con hipótesis
Un sistema de pagos **va** a fallar: se cae un proceso, la red se pone lenta, llega un mensaje corrupto. La pregunta no es si falla, sino **qué le pasa a las decisiones cuando falla**. Cada prueba sigue el método de *chaos engineering*:
1. **Estado estable:** carga al ~50% del throughput sostenido (§5.4), con el p95 y el lag estables.
2. **Hipótesis** escrita **antes** de la prueba ("si muere el TaskManager, se recupera en < 30 s sin perder eventos").
3. **Inyección** controlada de la falla.
4. **Observación** con métricas, trazas y el verificador de corrección.
5. **Resultado:** hipótesis confirmada o refutada. Si se refuta, se corrige y se documenta.

### 7.2 Herramientas
| Herramienta | Para qué |
|---|---|
| `docker kill` / `stop` / `pause` | Matar o congelar contenedores |
| **Toxiproxy** (contenedor Go de ~20 MB) | Proxy entre el scorer y Redis o Kafka para inyectar latencia, cortes o ancho de banda limitado. Funciona en Docker Desktop sobre Windows sin `tc`/`NET_ADMIN` |
| `chaos/run.py` | Orquesta: carga estable → inyección en t = 60 s → 5 min de observación → reporte |
| `chaos/verify.py` | **Verificador de corrección:** compara los `payment_id` producidos (el generador los registra en Parquet) con las decisiones en Postgres y reporta perdidos, duplicados y desorden por usuario |

`make chaos EXP=F1` produce `docs/resilience/F1.md` con la línea de tiempo (gráfica con la marca de la falla), el tiempo de recuperación y los conteos de corrección.

### 7.3 Catálogo de fallas
| ID | Falla | Hipótesis | Qué se mide | Mecanismo que la resuelve |
|---|---|---|---|---|
| **F1** | Muere el TaskManager de Flink | Reinicia desde el último checkpoint en < 30 s; **0 perdidos**; duplicados acotados | Tiempo hasta que el lag vuelve al nivel base; perdidos y duplicados | Checkpoints + reprocesamiento desde los offsets del checkpoint; idempotencia aguas abajo |
| **F2** | Muere 1 de N procesos del scorer | Rebalanceo en < 5 s; los demás toman sus particiones | Pausa de rebalanceo, pico de p95, duplicados | Consumer groups con asignador `cooperative-sticky` (rebalanceo incremental) |
| **F3** | Redis caído | El scorer **no se detiene**: entra en modo degradado | Tasa de decisiones degradadas, cambio en la mezcla de decisiones | Timeout de 5 ms + circuit breaker en Redis + política degradada (ver §7.4) |
| **F4** | Redis lento (+20 ms vía Toxiproxy) | El timeout corta y se activa el modo degradado sin romper el p95 | p95, tasa de timeouts | Timeout dentro del presupuesto de latencia |
| **F5** | Reinicio del broker de Kafka (en `lite` es una caída total) | El generador acumula en buffer y el sistema se pone al día sin perder | Backlog máximo, tiempo de puesta al día, perdidos | Productor idempotente (`enable.idempotence=true`), reintentos, buffer del productor |
| **F6** | Mensajes corruptos (*poison pills*) | Van a la DLQ y el pipeline **no se detiene** | Conteo en la DLQ, throughput sin cambio | Validación de esquema + DLQ (ver nota) |
| **F7** | MLflow caído + modelo corrupto en hot-reload | El scorer sigue con el modelo cacheado y **rechaza** el corrupto | Decisiones sin interrupción, versión activa | Cache local + validación previa al cambio (firma + *smoke test* de paridad) + rollback automático |
| **F8** | Gateway LLM caído | El explicador pasa a plantilla y el camino crítico no se entera | p95 sin cambio, tasa de fallback | Circuit breaker del gateway + fallback de plantilla (§3.10) |
| **F9** | Ráfaga de sobrecarga (2× el sostenido durante 60 s) | El lag crece sin caídas y se recupera al bajar la carga | Lag máximo, tiempo de recuperación, p95 durante y después | Kafka como buffer; degradación: apagar shadow y explainer (ver §7.4) |
| **F10** | F1 en modo **exactly-once** | **0 duplicados** visibles para consumers `read_committed` | Duplicados = 0; latencia mayor (cuantifica el ADR-4) | Sink transaccional + checkpoints |
| F1b–F5b | Las mismas con 2 TaskManagers y 3 brokers con réplicas | Sin caída total en F5; F1 con failover | Ídem | `⏳ 16GB` |

**Nota F6:** `json.ignore-parse-errors` en Flink SQL **descarta en silencio** los mensajes inválidos. Eso es pérdida invisible, inaceptable en pagos. Se valida en el borde (el generador o un validador ligero publica los inválidos en `dlq`) y Flink se configura para fallar ruidosamente si algo pasa el filtro. Hay que verificar en la implementación qué opciones de manejo de errores ofrece la versión de Flink usada.

### 7.4 Decisión de negocio: ¿*fail-open* o *fail-closed*?
Cuando el sistema está degradado (sin perfil de Redis), ¿se aprueba todo para no frenar ventas (*fail-open*) o se bloquea todo por seguridad (*fail-closed*)? Ninguno de los extremos sirve. La política del proyecto es:
- Usar valores **neutros** de perfil y umbrales **más conservadores**, marcando la decisión con `degraded=true`.
- Montos por encima de un límite pasan a `REVIEW`, nunca a `APPROVE` automático.
- Ante una sobrecarga (F9), el orden de sacrificio es: explainer → shadow → feature writer. **El scoring nunca se sacrifica**, y los pagos no se descartan (*load shedding* no aplica en pagos).

Esta discusión es un clásico de las entrevistas de system design, y aquí queda respaldada con datos de F3 y F9.

### 7.5 Runbooks
Cada falla genera una entrada en `docs/runbooks.md` con este formato: **Síntoma → Dashboard o alerta que la detecta → Diagnóstico → Acción → Verificación.** Conecta con el curso `09/39 - Production Incident Response for AI Systems`.

### 7.6 Plantilla de resultados (README)
| ID | Falla | Hipótesis | Recuperación | Perdidos | Duplicados | Resultado |
|---|---|---|---|---|---|---|
| F1 | TaskManager muere | < 30 s, 0 perdidos | — s | 0 | — | ✅/❌ |
| F3 | Redis caído | Modo degradado | — | 0 | — | ✅/❌ |
| … | | | | | | |

### 7.7 Aporte al README
Sección **Resilience & Failure Testing**: el método, la tabla de resultados, la discusión fail-open/fail-closed y la gráfica de línea de tiempo de F1. Los runbooks quedan enlazados.

---

## 8. Presupuesto de recursos

> ⚠️ Son **estimaciones iniciales**. El primer hito de implementación las valida con `docker stats` y se ajustan aquí con los valores reales.

### 8.1 De dónde sale la memoria (8 GB)
```
8 GB físicos
├── Windows + apps mínimas ............ ~2.5 GB  (cerrar el navegador pesado durante el benchmark)
├── Ollama nativo en Windows .......... ~0.4 GB RAM (+ ~1–1.3 GB de VRAM)  → solo en el perfil explain
└── VM de WSL2 (límite en .wslconfig) .. 5 GB
    ├── Docker Engine + overhead ...... ~0.5 GB
    └── Contenedores .................. ~4.3 GB disponibles  ← el presupuesto real
```
`%UserProfile%\.wslconfig` (configuración de Windows, sin administración de Linux):
```ini
[wsl2]
memory=5GB
processors=6   # i5-10300H: 8 hilos → 2 para Windows
swap=2GB        # red de seguridad; si se usa durante el benchmark, la corrida no es válida
```
**Ollama corre nativo en Windows**, no en Docker: tiene acceso directo a la GPU y **no consume del límite de la VM**. Los contenedores lo alcanzan en `host.docker.internal:11434`.

### 8.2 Perfiles de Compose y su presupuesto
Cada perfil se levanta solo, con `make down` entre perfiles. Todos los contenedores tienen `mem_limit` y las JVM tienen **heap explícito** (el límite del contenedor no es el heap).

**`core`: benchmark del camino crítico (E1–E3, E5–E7, E9–E10)**
| Servicio | Configuración clave | Límite |
|---|---|---|
| Kafka (KRaft) | `KAFKA_HEAP_OPTS=-Xmx512m -Xms512m` | 850 MB |
| Flink JobManager | `jobmanager.memory.process.size: 600m` | 650 MB |
| Flink TaskManager | `taskmanager.memory.process.size: 1200m`, 4 slots, managed memory para RocksDB | 1.25 GB |
| Redis | `maxmemory 256mb` (uso real ~50 MB) | 200 MB |
| Scorer × 2 procesos | ONNX Runtime, 1 thread cada uno | 400 MB |
| Generador × 2 procesos | | 200 MB |
| Latency Collector | HdrHistogram | 150 MB |
| Prometheus | Retención de 6 h, scrape cada 5 s | 350 MB |
| kafka-exporter + redis_exporter | | 60 MB |
| **Total** | | **≈ 4.1 GB** ✅ (~0.2 GB de margen) |

**`ops`: demos, shadow, drift y dashboards (E8, §4.10–4.11)**
Se baja la carga: **1 generador y 1 scorer** (−0.3 GB), y se suman feature writer (100 MB), auditor (100 MB), Postgres (250 MB, `shared_buffers=128MB`), API (150 MB), shadow scorer (200 MB) y Grafana (150 MB). **Total ≈ 4.75 GB** 🟡: entra subiendo `memory=5.5GB` en `.wslconfig` y con el navegador abierto solo en Grafana. Es para **demostrar**, no para medir throughput.

**`explain`: el explicador con LLM**
`ops` sin el shadow, más explainer (150 MB) y `llm-gateway` (~150 MB sin caché semántica). El LLM local (Gemma 3 1B o Qwen3 1.7B en Q4) corre en **Ollama nativo**, con ~1–1.3 GB de VRAM. **Total en la VM ≈ 4.7 GB** 🟡.

**`chaos`: pruebas de falla (F1–F10)**
`core` con 1 generador, más Toxiproxy (20 MB), auditor (100 MB) y Postgres (250 MB) para el verificador. **Total ≈ 4.3 GB** ✅.

**`train`: entrenamiento offline (§4) — nunca con el stack de streaming arriba**
| Proceso | Memoria | Nota |
|---|---|---|
| MLflow (SQLite) | 200 MB | |
| Profile Builder (Polars lazy) | ~1 GB pico | Procesa el histórico en streaming |
| XGBoost con `QuantileDMatrix` | ~1–1.5 GB con 3–5 M filas | El `QuantileDMatrix` reduce la memoria frente al `DMatrix` clásico. Si no alcanza, 3 M filas |
| Optuna | Igual que XGBoost | Trials secuenciales, no en paralelo |
| MLP en PyTorch | ~0.5 GB RAM + < 1 GB VRAM | En la GPU de 4 GB |

**`serving-triton`: Variante B (E4)** 🟡 / `⏳ 16GB`
El scorer pasa a cliente gRPC (−0.2 GB) y se suma Triton CPU con el backend FIL u ONNX (~1–1.5 GB). Con 8 GB solo se pueden medir **escalones bajos** con 1 generador. La comparativa completa queda pendiente para 16 GB. ⚠️ **Disco:** la imagen de Triton pesa varios GB; se verifica el espacio antes de bajarla.

### 8.3 VRAM (4 GB): quién la usa
| Uso | VRAM | Cuándo |
|---|---|---|
| Ollama + Gemma 3 1B / Qwen3 1.7B (Q4) | ~1–1.3 GB | Perfil `explain` |
| Entrenamiento del MLP challenger | < 1 GB | Perfil `train` |
| **Scoring en producción** | **0** | Decisión deliberada: con lotes pequeños, el costo de copiar datos a la GPU supera la ganancia. XGBoost y el MLP en ONNX van en CPU. Se documenta el razonamiento |

### 8.4 CPU: el verdadero límite probable
**Hardware declarado:** Intel Core i5-10300H (10.ª gen, **4 núcleos / 8 hilos**, 2.5 GHz base / 4.5 GHz turbo, 45 W).

Con RAM controlada, lo más probable es que el techo de throughput lo ponga la **CPU**. Kafka, Flink, el generador y el scorer compiten por los mismos 8 hilos. Reparto inicial:

| Consumidor | Hilos aprox. | Ajuste |
|---|---|---|
| Windows + Docker Desktop | 2 | `.wslconfig processors=6` |
| Flink TaskManager | 2 | Paralelismo 2–4 (los slots comparten núcleos) |
| Kafka | 1 | `num.network.threads=2`, `num.io.threads=2` |
| Scorer × 2 | 2 | 1 thread de ONNX Runtime por proceso |
| Generador × 2 | 1–2 | Si `gen_lag` crece, el generador compite con lo que mide (§5.3) |

**Estimación realista (a validar en E1):** un throughput sostenido de **~3k–8k ev/s** con p95 ≤ 80 ms en `core`. Los 20k ev/s son muy improbables en este equipo con todo corriendo junto, sea cual sea la RAM. Con 16 GB mejora la memoria, **no** la CPU. Una CPU de laptop con H de 45 W es sensible al calor: la higiene de §5.5 importa.

Además:
- El manifiesto de cada corrida registra el modelo de CPU, los núcleos y los threads (§5.5).
- E10 (escalado) mide dónde satura cada etapa.
- Optimización documentada: generador o scorer en **Go** si Python es el cuello de botella (reutiliza tu stack de Go).

### 8.5 Disco
Imágenes ~5 GB sin Triton (Kafka, Flink, Prometheus, Grafana, Postgres, Python). Triton suma varios GB más. Datos: histórico en Parquet (~0.5–1 GB), resultados y logs pequeños. Las retenciones cortas de Kafka (1–24 h) evitan que crezca.

### 8.6 Salvaguardas
- **`make doctor`:** antes de levantar un perfil verifica el límite de la VM, la RAM libre, el disco y que no haya otro perfil corriendo. Si no se cumple, aborta con un mensaje claro.
- `mem_limit` en **todos** los servicios: si alguno se excede, el OOM mata solo a ese contenedor, no a la VM entera.
- Regla de validez (§5.5): si se usa swap durante una medición, la corrida se descarta.

### 8.7 Perfil `full` (`⏳ 16GB`, `.wslconfig memory=11GB`)
| Cambio | Memoria adicional aprox. |
|---|---|
| 3 brokers de Kafka con réplica 3 | +1.7 GB |
| Flink con 2 TaskManagers de 2 GB y paralelismo 8–12 | +2.8 GB |
| Scorer × 4 y generador × 4 | +0.6 GB |
| Todos los servicios async + Grafana siempre activa | +1 GB |
| Tempo + Loki + OTel Collector + cAdvisor | +1 GB |
| Kafka UI, Postgres como backend de MLflow, Triton | +2 GB |
Con esto se habilitan E1-full (20k+ ev/s), E4 completo, F1b–F5b y la observabilidad completa.

### 8.8 Aporte al README
Sección **Hardware Requirements & Profiles**: tabla de perfiles con su RAM, el `.wslconfig` recomendado, qué se puede correr con 8 GB vs 16 GB y la explicación de por qué el scoring va en CPU.

---

## 9. Ejecución local y alternativas de despliegue

### 9.1 Requisitos
| Herramienta | Para qué | Instalación en Windows |
|---|---|---|
| Docker Desktop (backend WSL2) | Todo el stack | Instalador oficial |
| Git | Repo | `winget install Git.Git` |
| Python 3.12 + **uv** | Entrenamiento, bench, tests y desarrollo local de los servicios | `winget install astral-sh.uv` |
| make | Atajos (`make up`, `make bench`…) | `winget install ezwinports.make` (o usar los comandos equivalentes del README) |
| Ollama | LLM local del explicador (solo el perfil `explain`) | Instalador oficial para Windows |
| Driver NVIDIA | GPU para Ollama y el MLP | Ya instalado |
| `llm-gateway` | Gateway LLM propio en Python (P0) | Imagen construida desde `../llm-gateway` (`make gateway-build`) |

### 9.2 Inicio rápido (de cero a la primera cifra)
```bash
git clone https://github.com/Leito2/<repo-p1> && cd <repo-p1>
cp .env.example .env            # valores por defecto pensados para 8 GB
make doctor                     # verifica la RAM de la VM, el disco y los puertos
make setup                      # uv sync + descarga de imágenes
make data                       # histórico sintético (Parquet) + perfiles
make train                      # entrena el champion y lo registra como 'champion' en MLflow
make up PROFILE=core            # levanta el camino crítico
make smoke                      # 2k ev/s durante 2 min → imprime p50/p95/p99
make down
```
`make smoke` es la verificación de "funciona en mi máquina": en ~10 minutos desde el clon hay una cifra real.

### 9.3 Flujos de uso
| Flujo | Comandos | Resultado |
|---|---|---|
| Benchmark para el CV | `make up PROFILE=core` → `make bench EXP=E1 REPS=3` → `make report` | Tabla y gráficas en `docs/results/` |
| Otro experimento | `make bench EXP=E3` (aplica su configuración) | Comparativa en `docs/results/E3/` |
| Demo con dashboards | `make up PROFILE=ops` → abrir `http://localhost:3000` | D1–D4 en vivo |
| Demo de drift | `make up PROFILE=ops` → `make drift-demo` | Inyecta el patrón nuevo, reentrena, shadow y promoción (§4.11) |
| Explicaciones con LLM | `ollama pull gemma3:1b` → `make up PROFILE=explain` | Topic `explanations` + D5 |
| Pruebas de falla | `make up PROFILE=chaos` → `make chaos EXP=F1` | `docs/resilience/F1.md` |
| Variante Triton | `make up PROFILE=serving-triton` → `make bench EXP=E4` | ONNX vs Triton |
| Promover un modelo | `make promote MODEL=challenger` | El scorer recarga el modelo en caliente |

### 9.4 Configuración
Todo se configura por `.env` y YAML versionado (sin valores mágicos en el código):
- `.env`: `PROFILE`, `RATE`, `LLM_PROVIDER=mock|ollama`, `LLM_GATEWAY_URL`, `OLLAMA_HOST=http://host.docker.internal:11434`, `OTEL_SAMPLING=0.01`.
- `config/`: `thresholds.yaml`, `flink.yaml` (buffer-timeout, paralelismo, checkpoints), `scorer.yaml` (lote y timeout), `experiments/E*.yaml` (cada experimento es un archivo).
- Por defecto `LLM_PROVIDER=mock`: **ningún flujo llama a un LLM real** a menos que se pida explícitamente.

### 9.5 Desarrollo diario (iteración rápida)
La infraestructura va en Compose (`make up PROFILE=core-infra`: Kafka, Flink, Redis) y el servicio en el que se trabaja corre **nativo** desde el IDE con `uv run scorer --dev`, apuntando a `localhost`. Así no hay que reconstruir imágenes con cada cambio.
`make test` corre los unit tests, la paridad ONNX y las features derivadas, sin Docker. `make test-integration` levanta `core` a carga baja y corre el test de paridad de features con Flink (§4.3).

### 9.6 Alternativas de ejecución y despliegue
| Opción | Cuándo usarla | Costo | Estado en el proyecto |
|---|---|---|---|
| **A · Docker Compose local** | Siempre: es el camino principal | $0 | ✅ Implementado |
| **B · Infra en Compose + servicios nativos** | Desarrollo (§9.5) | $0 | ✅ Implementado |
| **C · GitHub Codespaces** | Para correr el perfil `full` **antes** del upgrade a 16 GB: las máquinas de 4 núcleos tienen 16 GB de RAM | $0 dentro de la cuota gratis mensual de core-hours (verificar la cuota vigente; una máquina de 4 núcleos la consume 4 veces más rápido) | 📄 Documentado + `devcontainer.json` incluido |
| **D · Grafana Cloud (free tier)** | Mostrar dashboards en público: el Prometheus local hace *remote write* | $0 dentro de los límites de series del free tier | ⚪ Opcional |
| **E · Una VM cloud** | Medir en hardware dedicado y comparable | Por hora (spot) | 📄 Solo documentado: por regla, solo P3 usa cloud |
| **F · Servicios gestionados (producción real)** | "¿Cómo lo llevarías a producción?" (entrevista) | — | 📄 Tabla de mapeo (abajo) |
| **G · Kubernetes (kind/k3d + Helm)** | Orquestación y autoescalado | $0 local, pero RAM extra | 📄 Trabajo futuro (los cursos de K8s ya están en el vault) |

**Mapeo a servicios gestionados (opción F):**
| Pieza local | AWS | GCP | Agnóstico / SaaS |
|---|---|---|---|
| Kafka | MSK | Managed Service for Apache Kafka | Confluent Cloud, Redpanda Cloud |
| Flink | Managed Service for Apache Flink | Dataproc (Flink) | Confluent Cloud for Flink |
| Redis | ElastiCache | Memorystore | Redis Cloud |
| Scorer / Triton | SageMaker endpoints / EKS | Vertex AI endpoints / GKE | — |
| MLflow | SageMaker MLflow | Vertex AI Model Registry | Databricks MLflow |
| Prometheus + Grafana | Managed Prometheus + Managed Grafana | Managed Prometheus + Cloud Monitoring | Grafana Cloud |

> Los nombres de servicios gestionados cambian seguido; se verifican al redactar el README.

### 9.7 Troubleshooting (problemas típicos en Windows y Docker)
| Síntoma | Causa | Solución |
|---|---|---|
| Contenedores mueren con `OOMKilled` | `.wslconfig` no aplicado | `wsl --shutdown` y reiniciar Docker Desktop |
| Los clientes no conectan a Kafka desde el host | *Advertised listeners* (dentro vs fuera de Docker) | Dos listeners: `INTERNAL://kafka:29092` y `EXTERNAL://localhost:9092` |
| El explainer no ve a Ollama | Ollama corre en el host | Usar `host.docker.internal:11434` y `OLLAMA_HOST=0.0.0.0` en Windows |
| Scripts `.sh` fallan dentro de los contenedores | Finales de línea CRLF | `.gitattributes` con `*.sh text eol=lf` |
| Rutas largas o nombres inválidos | Límites de Windows | `git config core.longpaths true`; nombres sin `:` |
| El job de Flink no arranca | Slots insuficientes o falta el JAR del conector | Revisar la UI de Flink (`:8081`) y el paralelismo ≤ slots |
| p95 errático entre corridas | *Thermal throttling* o batería | Higiene de §5.5 |

### 9.8 Aporte al README
Secciones **Quickstart**, **Running Experiments**, **Configuration**, **Deployment Options** (con la tabla de mapeo a producción) y **Troubleshooting**.

---

## 10. Estructura del repo y del README

### 10.1 Nombre del repo
Propuesta: **`realtime-fraud-detection`**, descriptivo y fácil de encontrar en búsquedas. Alternativa con marca: `fraudstream`.

### 10.2 Árbol del repo
```
realtime-fraud-detection/
├── README.md                     ← el documento principal (§10.4)
├── LICENSE                       (MIT)
├── Makefile                      ← todos los atajos de §9
├── docker-compose.yml            ← un solo archivo con profiles: core, ops, explain, chaos, train, serving-triton, full
├── .env.example
├── pyproject.toml / uv.lock      ← workspace de uv (un lock para todo)
├── .gitattributes                (*.sh eol=lf)
├── .github/workflows/ci.yml
├── .devcontainer/devcontainer.json   (opción C: Codespaces)
│
├── packages/
│   └── fraudcore/                ← código compartido: la clave contra el skew
│       ├── features.py           (especificación + features derivadas + orden de columnas)
│       ├── contracts.py          (esquemas de eventos con msgspec/pydantic)
│       ├── model_io.py           (cargar desde el registry, caché, validación previa al swap)
│       ├── thresholds.py
│       └── telemetry.py          (métricas, OTel, headers de Kafka)
│
├── services/                     ← cada uno: src/ + tests/ + entrypoint
│   ├── generator/
│   ├── scorer/                   (--mode champion | shadow | triton-client)
│   ├── api/
│   ├── feature_writer/
│   ├── profile_builder/
│   ├── explainer/
│   ├── auditor/
│   └── latency/
├── docker/
│   └── app.Dockerfile            ← UNA imagen de Python para todos los servicios (cambia el entrypoint)
│
├── flink/
│   ├── sql/                      (features_10m.sql, features_long.sql, topics DDL)
│   ├── pyflink/                  (opción B del spike S1)
│   └── conf/                     (flink-conf por perfil)
│
├── training/
│   ├── train_xgb.py · tune.py · train_mlp.py
│   ├── export_onnx.py · evaluate.py · rules_baseline.py
│   └── notebooks/eda.ipynb       (exploración del dataset sintético)
│
├── infra/
│   ├── kafka/create-topics.sh
│   ├── postgres/init.sql         (tablas + vistas del auditor)
│   ├── redis/redis.conf
│   ├── mlflow/
│   ├── triton/model_repository/
│   └── toxiproxy/
│
├── observability/
│   ├── prometheus/prometheus.yml · alerts.yml
│   ├── grafana/provisioning/ · dashboards/*.json
│   └── otel/collector.yaml
│
├── config/
│   ├── thresholds.yaml · scorer.yaml · flink.yaml
│   └── experiments/E1.yaml … E10.yaml
│
├── bench/                        (run.py, report.py, stats_sampler.py)
├── chaos/                        (run.py, verify.py, experiments/F1.yaml…)
├── scripts/doctor.py
├── tests/
│   ├── integration/              (paridad de features con Flink, smoke de punta a punta)
│   └── contract/                 (esquemas de los eventos)
│
└── docs/
    ├── adr/                      (ADR-001…007 completos)
    ├── results/                  (generado por bench/report.py)
    ├── resilience/               (generado por chaos/run.py)
    ├── runbooks.md
    └── images/                   (capturas, GIFs, diagramas exportados)
```

**Decisiones de estructura:**
- **Una sola imagen de Python** para todos los servicios: ahorra disco y tiempo de build, y garantiza las mismas versiones de librerías en todas partes.
- **`fraudcore` compartido:** el entrenamiento, el scorer, la API y el explainer importan las mismas funciones de features. Es la defensa estructural contra el *train/serve skew* (§4.3).
- **Un `docker-compose.yml` con profiles**, en vez de varios archivos: una sola fuente de verdad.
- Los resultados en `docs/` son **generados**, no escritos a mano: el README enlaza a datos reproducibles.

### 10.3 Qué va en el README y qué en `docs/`
| README (narrativa completa) | `docs/` (detalle y datos crudos) |
|---|---|
| Todo el contexto teórico, conceptual y macro | El texto completo de cada ADR |
| Cada componente, de concepto a técnico | Todas las corridas y repeticiones |
| Tablas de resultados resumidas + gráficas clave | Reportes completos de cada experimento y falla |
| Cómo correrlo, alternativas, troubleshooting | Runbooks |

El README se entiende **solo**, sin abrir `docs/`. `docs/` es para quien quiere verificar.

### 10.4 Esqueleto del README (en inglés)
Cada sección indica el módulo del plan que la alimenta y el hito (§11) en que se escribe.

```markdown
# 🛡️ Real-time Fraud Detection Platform
> one-liner + badges (CI, license, Python, Flink) + the CV headline with real numbers
> GIF: Grafana during the drift demo

## TL;DR — Results at a Glance          ← §1.6, §5.9     (final milestone)
## Table of Contents

## Part I — The Big Picture (theory first)
### 1. The Problem: Fraud Is a Real-time Problem          ← §1.1
### 2. Core Concepts Primer                                ← NEW: theory for newcomers
    - Event streaming & logs (Kafka) · partitions & ordering
    - Stream processing: per-event vs micro-batch
    - Time in streaming: event vs processing time, watermarks
    - Stateful processing, checkpoints, delivery guarantees
    - Online vs offline features, train/serve skew
    - Tail latency: why p95/p99, coordinated omission
    - Champion/challenger, shadow mode, drift
### 3. What This Project Demonstrates                      ← §1.3
### 4. Architecture (Mermaid diagram + the two paths)      ← §2.1–2.2
### 5. Design Decisions (ADR summaries)                    ← §2.3
### 6. The Journey of a Payment                            ← §2.6

## Part II — Components (each: Concept → How it works here → Technical details)
### 7.1 Load Generator … 7.13 MLflow                       ← §3
### 8. Data Contracts & Topics                              ← §2.4–2.5

## Part III — The Model
### 9. Why XGBoost (and when not)                          ← §4.1
### 10. Features & Feature Parity                          ← §4.3
### 11. From Score to Decision: Cost-based Thresholds      ← §4.6
### 12. Offline Evaluation                                 ← §4.9
### 13. Model Lifecycle: Shadow, Promotion, Drift Demo     ← §4.10–4.11

## Part IV — Proof
### 14. How We Measure                                     ← §5.1–5.5
### 15. Results & Experiments (E1–E10)                     ← §5.6, §5.9
### 16. Bottleneck Analysis                                ← §5.7, §5.10
### 17. Observability (dashboards, SLOs, traces)           ← §6
### 18. Resilience & Failure Testing                       ← §7

## Part V — Run It Yourself
### 19. Hardware Requirements & Profiles                   ← §8
### 20. Quickstart                                         ← §9.2
### 21. Running Experiments & Demos                        ← §9.3
### 22. Configuration                                      ← §9.4
### 23. Deployment Options & Path to Production            ← §9.6
### 24. Troubleshooting                                    ← §9.7
### 25. Project Structure                                  ← §10.2

## Part VI — Reflection
### 26. Lessons Learned
### 27. Limitations & Future Work
### 28. Glossary
### 29. References & License
```

**Reglas de estilo del README:**
- Diagramas en **Mermaid** (GitHub los renderiza) más una exportación PNG en `docs/images/` para LinkedIn.
- Cada sección de componente cierra con un bloque `<details><summary>Technical details</summary>`, para que la lectura macro fluya y el detalle técnico esté a un clic.
- Las cifras **siempre** llevan el hardware y el enlace al manifiesto de la corrida.
- Glosario al final: ningún término queda sin explicar para un lector no especializado.

### 10.5 Aporte al README
La sección **Project Structure** y el esqueleto completo, que se crea vacío en el primer hito y se va llenando en cada uno (§11).

---

## 11. Hitos de implementación y criterios de aceptación

### 11.1 Principios
- **Rebanadas verticales:** desde M1 el sistema funciona de punta a punta, aunque sea mínimo. Cada hito agrega profundidad, nunca piezas sueltas sin conectar.
- **Cada hito termina con algo demostrable** y con su parte del README escrita. El README crece con el código, no se escribe al final.
- **Prerrequisitos del vault:** el curso correspondiente se termina antes del hito que lo aplica (ver la columna "Curso").
- Tamaño relativo: **S** (1–2 sesiones), **M** (3–4), **L** (5+).

### 11.2 Definición de terminado (aplica a todos los hitos)
- [ ] CI en verde (lint, tests unitarios y de contratos; integración cuando aplique)
- [ ] `make doctor` y el perfil afectado levantan en 8 GB sin `OOMKilled`
- [ ] Las secciones del README del hito están escritas (teoría → técnico)
- [ ] Configuración en YAML/`.env`, sin valores mágicos
- [ ] **$0 gastados** (`LLM_PROVIDER=mock` u `ollama`)
- [ ] Commit y tag del hito (`m1`, `m2`…)

### 11.3 Hitos
| Hito | Objetivo | Tareas principales | Criterios de aceptación | README | Curso | Tamaño |
|---|---|---|---|---|---|---|
| **M0 · Bootstrap** | Repo listo para construir | Workspace de uv, `fraudcore` vacío, Compose con profiles (esqueleto), `doctor.py`, CI, `.gitattributes`, devcontainer, esqueleto del README | `make doctor` pasa; CI en verde; el README tiene todas sus secciones | §1 Problem, §2 Core Concepts (borrador), §25 Structure | — | S |
| **M1 · Esqueleto andante** | Un pago viaja de punta a punta | Generador open-loop básico → Kafka (topics) → Flink SQL pass-through con 1 feature → scorer con regla dummy → `decisions` → Latency Collector mínimo; `make smoke` | `make smoke` imprime p50/p95/p99 a 2k ev/s; **`docker stats` real que reemplaza las estimaciones de §8** | §4 Architecture v1, §20 Quickstart v1 | C1 (notas 00–04) | M |
| **M2 · Features en tiempo real** | Las features correctas, rápidas y verificadas | **Spike S1** (opción A vs B); ventanas `OVER` de §4.3; `buffer-timeout`; Profile Builder + Redis; Feature Writer; **test de paridad de features** | El test de paridad pasa en la CI; S1 decidido con datos (ADR actualizado); E3 preliminar | §5 Design Decisions, §7 Generator/Kafka/Flink/Redis/Profile Builder, §10 Feature Parity | C1 completo, C2, C3 | L |
| **M3 · El modelo** | Champion servido desde el registry | Histórico offline; baseline de reglas; XGBoost + Optuna; umbrales por costo; export a ONNX + paridad + benchmark; MLflow; carga del alias y hot-reload en el scorer | Tabla de evaluación offline (reglas vs XGB vs ONNX); paridad ONNX < 1e-5; el scorer decide con el champion; el hot-reload funciona sin perder eventos | §9 Why XGBoost, §11 Thresholds, §12 Offline Evaluation, §7 Scorer y MLflow | C4 (notas 00–01) | L |
| **M4 · La cifra** | **Primera cifra honesta para el CV** | `bench/run.py`, `report.py`, `stats_sampler`; validación de `gen_lag`; manifiestos; E1 (3 repeticiones), E2, E3 | Tabla de resultados generada; gráfica del codo; throughput sostenido definido con las 4 condiciones de §5.4; **primera versión de la frase del CV** | §14 How We Measure, §15 Results v1, §16 Bottleneck Analysis v1, TL;DR v1 | C6 (nota 04) | M |
| **M5 · Observabilidad** | Operar y diagnosticar en vivo | Catálogo de métricas; dashboards D1–D3 provisionados; reglas de alerta + SLO burn rate; propagación de OTel por headers (1%); E9 | Los dashboards cargan solos; una alerta se dispara de forma controlada; una traza de punta a punta visible (`full` u opcional en `lite`); E9 cuantificado | §17 Observability | C6 completo | M |
| **M6 · MLOps online** | El ciclo de vida del modelo en vivo | Auditor + Postgres + vistas; challenger MLP (PyTorch → ONNX) en shadow; D4; `make promote`; **demo de drift** + GIF; E8 | Champion vs challenger con etiquetas reales; E8 ≈ 0 de impacto; la demo de drift es reproducible con un comando; GIF grabado | §13 Model Lifecycle, §7 Shadow/Auditor | C4 (nota 04) | L |
| **M6b · Backtesting y gobernanza** (§13) | Evaluación offline reproducible sobre datos versionados | job batch en contenedor (componente KFP ejecutado con `kfp.local`) sobre MinIO; Evidently; reportes Pandas; *dataset card* | `make backtest` produce champion vs challenger desde MinIO; `make drift-report` genera el reporte de Evidently | §13 Model Lifecycle (gobernanza) | — | M |
| **M7 · Explainer** | "XGBoost decide, el LLM redacta" | SHAP + prompt; gateway con `mock` → `ollama`; backpressure y muestreo; fallback de plantilla; D5; API `GET /decisions/{id}` | Explicaciones en `explanations` y Postgres; el p95 del camino crítico **no cambia** con el explainer activo; el fallback funciona con el gateway caído | §7 Explainer y API | — | M |
| **M7c · gRPC** (§14) | Scoring síncrono para el checkout | `scoring.proto` (contrato ya en el repo), servidor `grpc.aio`, cliente con deadlines, health, reflection, interceptores, `buf` en la CI, experimento E11 | `grpcurl` llama a los 3 métodos; deadline vencido → `DEADLINE_EXCEEDED` (test); tabla REST vs gRPC publicada | §14 gRPC | C5 (`10/48`) | M |
| **M8 · Resiliencia** | Fallar con elegancia y demostrarlo | Toxiproxy; `chaos/run.py` + `verify.py`; F1–F10; política de modo degradado; runbooks; E5 | Las 10 fallas ejecutadas con hipótesis confirmadas o refutadas y documentadas; 0 perdidos en F1/F2/F5; tabla de resiliencia | §18 Resilience, runbooks | C1 (nota 03) | L |
| **M9 · Experimentos restantes** | Completar los trade-offs | E4 (Triton en escalones bajos), E6, E7, E10 | Cada experimento con su gráfica y conclusión de 2–3 líneas | §15 Results completo, §16 v2 | C4 completo, C2 (nota 04) | M |
| **M10 · Pulido y publicación** | Listo para recruiters | Lessons learned, limitations, glosario, Mermaid + PNG, frase final del CV, revisión completa del README, tag `v1.0` | Una persona ajena lo levanta desde el README sin ayuda (prueba con alguien o en Codespaces); frase del CV con cifras y hardware | §26–29, TL;DR final | — | M |
| **M11 · `⏳ 16GB`** | Cifras con el hardware mejorado | Perfil `full`; E1-full; F1b–F5b; E4 completo; observabilidad completa | Resultados `v1.1` publicados junto a los de 8 GB (no los reemplazan: la comparación también cuenta una historia) | §15, §18, §19 actualizados | — | M |

### 11.4 Ruta crítica y puntos de decisión
```
M0 → M1 → M2 ──► M3 → M4 (🎯 primera cifra del CV)
              ▲          └─► M5 → M6 → M7 → M8 → M9 → M10 (v1.0) ··· M11 (⏳16GB)
       Spike S1: decide la arquitectura del job de Flink
```
| Punto de decisión | Cuándo | Qué se decide |
|---|---|---|
| **D1** | Final de M1 | ¿Kafka o Redpanda? Según la RAM real medida con `docker stats` |
| **D2** | Mitad de M2 | Spike S1: features con opción A o B |
| **D3** | Final de M4 | ¿Python alcanza o el scorer o generador pasan a Go? Según el cuello de botella de E1 |
| **D4** | Final de M4 | ¿Se ajustan las metas? Con la cifra real, se define la frase honesta del CV |

### 11.5 Hitos opcionales (si sobra tiempo o para la versión 2)
- Scorer en Go con ONNX Runtime, comparado contra el de Python.
- Avro con Schema Registry (§2.3, ADR-7).
- Canary real (un porcentaje del tráfico al challenger) después del shadow.
- Grafana Cloud free con dashboards públicos (§9.6 D).
- Integración con P2: el router consulta `GET /decisions/{id}` ante una disputa de pago.

---

## 12. Riesgos y pendientes

### 12.1 Registro de riesgos
| ID | Riesgo | Prob. | Impacto | Mitigación | Señal temprana |
|---|---|---|---|---|---|
| **R1** | La CPU (4C/8T) satura muy por debajo de la meta | Alta | Medio | Se publica la cifra honesta con su cuello de botella (§5.10); optimización en Go (D3); medición en Codespaces | E1 en M4 |
| **R2** | 8 GB no alcanzan; `OOMKilled` | Media | Alto | Perfiles (§8), `doctor`, `mem_limit`, Redpanda como plan B (D1) | `docker stats` en M1 |
| **R3** | La limitación de `OVER` en Flink SQL complica las features | Alta (ya conocida) | Medio | Spike S1 con las opciones A, B y C (§3.3) | M2 |
| **R4** | PyFlink (opción B) requiere una imagen de Flink con Python: más disco y complejidad | Media | Bajo | Solo si S1 elige B; imagen derivada documentada | S1 |
| **R5** | Versiones del conector de Kafka incompatibles con la versión de Flink (los conectores suelen salir después del core) | Media | Medio | Fijar versiones validadas en M1; evitar la versión de Flink más reciente si su conector no está listo | M1 |
| **R6** | **Los datos sintéticos son demasiado fáciles**: PR-AUC ≈ 0.99 que no convence a nadie | **Alta** | **Alto** | Ver §12.2 | Primera evaluación en M3 |
| **R7** | *Train/serve skew* no detectado | Media | Alto | Test de paridad en la CI (§4.3) + `fraudcore` compartido | M2 |
| **R8** | Medición inválida (el generador compite por CPU, *throttling*) | Media | Alto | Reglas de validez de §5.3 y §5.5; manifiestos | `gen_lag` |
| **R9** | **Scope creep**: el proyecto es grande y se alarga sin terminar | Alta | Alto | MVP explícito y lista de recortes (§12.3) | Atraso de más de 2 hitos |
| **R10** | Particularidades de Docker Desktop en Windows (montajes lentos, CRLF, red) | Media | Bajo | Solo se montan configs (el código va en la imagen); `.gitattributes`; troubleshooting en §9.7 | M0–M1 |
| **R11** | La imagen de Triton (varios GB) llena el disco | Media | Bajo | Verificar el espacio antes; E4 completo para `⏳ 16GB` | M9 |
| **R12** | Dependencia de `llm-gateway` (P0): su M1–M2 debe estar listo antes del M7 de P1 | Baja | Medio | Interfaz compatible con OpenAI + proveedor `mock`; el explainer funciona sin el gateway (fallback a la plantilla) | M7 |

### 12.2 Mitigación de R6: que los datos sintéticos sean difíciles
Un detector perfecto sobre datos fáciles no demuestra nada. El generador debe incluir **casos legítimos que parecen fraude** y **fraude que parece legítimo**:
- **Legítimos difíciles:** viajeros reales (cambian de país), ráfagas legítimas (Black Friday, pago de servicios a fin de mes), dispositivos nuevos por cambio de celular, compras grandes ocasionales.
- **Fraude sutil:** fraudes "lentos" que imitan el ritmo del usuario, montos dentro del rango normal, el mismo país del usuario.
- **Ruido en las etiquetas:** un pequeño porcentaje de etiquetas erróneas (como en la realidad, donde algunos chargebacks son "fraude amigable").
- **Criterio:** si el PR-AUC del baseline de reglas supera ~0.9, el generador es demasiado fácil y se ajusta **antes** de entrenar el modelo final. La dificultad del dataset se documenta en el README.

### 12.3 MVP y lista de recortes
**MVP para el CV = M0 → M4** (la cifra) **+ la demo de drift de M6.**
Si el tiempo aprieta, se recorta en este orden (de lo primero que se sacrifica a lo último):
1. M9: experimentos restantes (E4, E6, E7, E10)
2. M7: explainer con LLM (queda la plantilla con SHAP)
3. M8 parcial: se conservan F1, F2, F3 y F6
4. M5 parcial: se conservan D1, D2 y las alertas; las trazas quedan para después

**Nunca se recorta:** M1–M4, el test de paridad de features y las reglas de honestidad de §5.10.

### 12.4 Verificar al implementar (supuestos técnicos del plan)
| Supuesto | Dónde | Hito |
|---|---|---|
| `COUNT(DISTINCT)` dentro de `OVER` en la versión de Flink usada | §3.3 | M2 |
| El conector de Kafka de Flink SQL lee y escribe `headers` como metadatos | §6.6 | M5 |
| Opciones de manejo de JSON inválido en Flink SQL | §7.3 F6 | M8 |
| El modo thread de PyFlink en la versión usada | §3.3 opción B | M2 |
| `onnxmltools` soporta la versión de XGBoost usada | §4.7 | M3 |
| kafka-exporter compatible con KRaft y la versión de Kafka | §6.2 | M5 |
| Cuota vigente de GitHub Codespaces | §9.6 C | Cuando se use |
| Nombres vigentes de los servicios gestionados | §9.6 F | M10 |

### 12.5 Pendientes `⏳ 16GB`
| Pendiente | Sección |
|---|---|
| Perfil `full` y E1-full (20k+ ev/s) | §5.6, §8.7 |
| E4 completo con Triton | §2.7, §8.2 |
| F1b–F5b con réplicas reales | §7.3 |
| Observabilidad completa (Tempo, Loki, cAdvisor, Grafana siempre activa) | §6.2 |
| Hito M11 y resultados `v1.1` | §11.3 |

### 12.6 Preguntas abiertas para el usuario
1. ~~El LLM Edge Gateway en Go del CV~~ → **resuelto:** se reemplazó por `llm-gateway` (P0, Python, desde cero), que trae API compatible con OpenAI, `mock`, streaming y tope de presupuesto por diseño.
2. ~~**Repo:** ¿público desde M0?~~ → **resuelto:** público desde M0 (`Leito2/realtime-fraud-detection`, MIT).


---

## 13. v2 — Absorción de los proyectos del CV en P1

> **Decisión (2026-10-06, ajustada):** los tres proyectos del CV quedan aparte, y su contenido se reparte en P0–P4 como componentes reales. En P1 entra la **gobernanza de modelos y datos** del *LLM Evaluation Suite*: drift, evaluación batch sobre datasets versionados y auditoría de alucinaciones. **Todo local:** el usuario descartó SageMaker (ni en la nube ni en modo local); los jobs corren como contenedores propios.

### 13.1 Mapa de absorción
| Origen (CV) | Elemento | Cómo existe en P1 | Hito |
|---|---|---|---|
| Evaluation Suite | Monitoreo de drift y observabilidad continua del modelo | **Evidently** sobre la ventana reciente de `decisions` + features servidas (Postgres) frente al baseline de entrenamiento: drift de datos (PSI, Wasserstein, chi²), drift de predicción y calidad cuando llegan etiquetas. Reportes HTML versionados y *test suites* que alimentan la alerta de drift de §6 y la demo de §4.11. Patrón: *baseline → constraints → schedule (cron local) → violations* | M6 |
| Evaluation Suite | Jobs de evaluación distribuida sobre datasets versionados en **S3** | **Backtesting offline** como job batch en contenedor (componente KFP ejecutado con `kfp.local`): lee el histórico Parquet desde **MinIO** (API S3, versionado de objetos), re-puntúa con champion y challenger y escribe métricas por segmento. El mismo contenedor corre con `docker run` o dentro del pipeline KFP de P2 | M6b |
| Evaluation Suite | Datasets versionados y gobernanza | MinIO como *data lake* local: `raw/`, `features/`, `training/` con versionado de objetos; manifiesto con hash de contenido; el digest del dataset va en cada corrida de MLflow (linaje); *dataset card* del generador sintético (distribuciones, tasa de fraude, casos difíciles, ruido de etiquetas) | M3, M6b |
| Evaluation Suite | Auditoría de alucinaciones | Las explicaciones del Explainer se auditan con `judgekit` (P2 §12): cada afirmación se contrasta con los valores SHAP y las features reales del pago ("monto 8× su promedio" debe coincidir con los datos). Métrica `explanation_faithfulness` y `unsupported_claim_rate`; compuerta antes de cambiar el prompt o el modelo del Explainer | M7 |
| Evaluation Suite | Pandas | Reportes de backtesting y de auditoría como DataFrames (Polars sigue en el Profile Builder por rendimiento; la comparación Polars vs Pandas en el mismo job se documenta) | M6b |
| Go Edge Gateway | Semantic cache, circuit breaker, fallback local | Heredados vía P0. El Explainer llama al alias `fast` con `X-Priority: batch` (el gateway lo descarta primero bajo carga) y `X-Cache-Scope: private` (cada explicación es única: **no** se cachea semánticamente; las guardas numéricas de P0 lo rechazarían igual) | M7 |

### 13.2 Cambios en el presupuesto de recursos
- `llm-gateway` en el perfil `explain`: ~450 MB (antes ~150 MB) porque la caché semántica y los guardrails son obligatorios en P0 y cargan sus modelos ONNX. Total del perfil `ops` ≈ 5 GB 🟡; si no entra, el Explainer usa el gateway del Compose standalone de P0.
- MinIO: ~150 MB, solo en el perfil `ops`.
- Evidently y el *Processing Job* local corren **bajo demanda** (`make drift-report`, `make backtest`), no como servicios permanentes.

### 13.3 Hitos nuevos o ampliados
| Hito | Cambio |
|---|---|
| M3 | El histórico offline se escribe en MinIO con manifiesto y digest en MLflow |
| M6 | + Evidently (reportes y *test suites*) como detector de la demo de drift |
| **M6b · Backtesting y gobernanza** (nuevo, M) | job batch en contenedor (componente KFP ejecutado con `kfp.local`) sobre MinIO; reportes Pandas por segmento; *dataset card*. Criterio: `make backtest` produce la tabla champion vs challenger desde MinIO, sin tocar el stack en vivo |
| M7 | + Auditoría de fidelidad de las explicaciones con `judgekit` y compuerta |

### 13.4 Frase del CV (agregado)
> … with Evidently drift monitoring, containerized backtests over versioned S3-compatible (MinIO) datasets, and LLM explanations audited for faithfulness (**{u}% unsupported claims**).


---

## 14. v3 — Scoring síncrono con gRPC

> **Decisión (2026-10-06):** el usuario pide implementar gRPC. P1 es el lugar natural: es el proyecto de **baja latencia**, y un pago con tarjeta real necesita una decisión **síncrona** dentro de un plazo (el checkout espera la respuesta), además del camino asíncrono por Kafka que ya existe.

### 14.1 Concepto
**gRPC** es un framework de llamadas a procedimientos remotos (RPC) sobre **HTTP/2** con mensajes **Protocol Buffers** (binarios, tipados y con esquema). Frente a REST con JSON:
- **Contrato primero:** el `.proto` define servicios y mensajes; el código cliente y servidor se genera. Cambiar el contrato sin romper clientes se verifica en la CI (`buf breaking`).
- **Menos bytes y menos CPU:** protobuf serializa más compacto y más rápido que JSON.
- **HTTP/2:** multiplexa muchas llamadas en una sola conexión y permite **streaming** en ambos sentidos.
- **Deadlines de primera clase:** el cliente dice cuánto puede esperar y el servidor lo ve; si el plazo vence, se cancela todo el camino.

Es el estándar para comunicación **servicio a servicio** de baja latencia (Triton, por ejemplo, ya expone gRPC). Para navegadores se sigue usando REST o SSE.

### 14.2 Cómo funciona aquí
```
 Pasarela de pagos (simulada) ──gRPC Score (deadline 50 ms)──► scoring-grpc ──► Redis (perfil + velocity) ──► ONNX ──► decisión
 Cliente de alto volumen ──gRPC ScoreStream (bidireccional)──► scoring-grpc (micro-lotes)
 P2 router (disputas) ──gRPC GetDecision──► scoring-grpc ──► Postgres
 Camino asíncrono (sin cambios): Kafka → Flink → scorer → decisions
```
- **Contrato:** `proto/fraud/v1/scoring.proto` con tres RPCs: `Score` (unario), `ScoreStream` (streaming bidireccional) y `GetDecision` (consulta).
- **Servidor:** `services/scoring_grpc/` con `grpc.aio` (asíncrono). Reutiliza `fraudcore` (mismas features e inferencia que el scorer de Kafka: sin *train/serve skew*). La velocity se lee de Redis (la que mantiene el Feature Writer).
- **Sin duplicar FastAPI:** la API REST de §3.7 se queda para demos y analistas; gRPC es el camino de máquina a máquina.

### 14.3 Detalle técnico
| Tema | Implementación |
|---|---|
| Generación de código | `grpcio-tools` (Python); `buf` para lint y detección de cambios incompatibles en la CI |
| Deadlines | El cliente fija 50 ms; el servidor revisa `context.time_remaining()` y responde `degraded=true` con features de respaldo si Redis no alcanza a responder |
| Resiliencia del cliente | *Service config* con reintentos solo para `UNAVAILABLE`, *keepalive*, *channel* reutilizado (nunca uno por request) |
| Errores | Códigos de estado gRPC (`INVALID_ARGUMENT`, `DEADLINE_EXCEEDED`, `UNAVAILABLE`) con detalles, no excepciones genéricas |
| Health y descubrimiento | `grpc.health.v1` (para Docker y balanceadores) y *server reflection* (para `grpcurl`) |
| Observabilidad | Interceptores de servidor: métricas de Prometheus por método y código, y spans de OTel con propagación de contexto en la metadata |
| Seguridad | TLS con certificado autofirmado en local (opcional) y API key por metadata |
| Streaming | `ScoreStream` agrupa en micro-lotes (hasta N mensajes o T ms) para usar la inferencia por lote de ONNX |

### 14.4 Experimento E11: REST vs gRPC vs streaming
Mismo modelo, misma máquina, carga open-loop (ghz para gRPC, k6 para REST):
| Variante | Qué se mide |
|---|---|
| REST + JSON (FastAPI) | p50/p95/p99, throughput, bytes por request, CPU |
| gRPC unario | Ídem |
| gRPC streaming bidireccional | Ídem, con distintos tamaños de micro-lote |

Resultado esperado (a confirmar): gRPC baja el overhead de serialización y de conexión; el streaming gana en throughput a costa de algo de latencia por el micro-lote. Si la diferencia es pequeña (la inferencia domina), también es un resultado honesto y se publica.

### 14.5 Hito y frase del CV
| Hito | Objetivo | Criterios de aceptación | Tamaño |
|---|---|---|---|
| **M7c · gRPC** (después de M4) | `scoring.proto`, servidor `grpc.aio`, cliente con deadlines, health, reflection, interceptores, `buf` en la CI, E11 | `grpcurl` llama a los 3 métodos; un deadline vencido devuelve `DEADLINE_EXCEEDED` (test); tabla E11 publicada | M |

> … exposes a **gRPC** scoring API (unary, bidirectional streaming, deadlines) next to the Kafka path: **p95 {g} ms vs {r} ms over REST/JSON** on the same model.

El contrato y su test ya están en el repo desde el M0 (`proto/fraud/v1/scoring.proto`, `tests/test_proto_contract.py`).
