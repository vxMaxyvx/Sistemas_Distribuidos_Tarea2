# Tarea 1 — Sistemas Distribuidos 2026-1
### Plataforma de analisis de consultas geoespaciales con cache

**Integrantes:** Vicente Cataldo, Maximiliano Oliva
**Stack:** Python 3.12, FastAPI, Redis 7.4, Docker Compose v2

Este repositorio implementa el **Entregable 1** de la Tarea 1: cuatro
servicios distribuidos (Generador de Trafico, Cache, Generador de Respuestas
y Almacenamiento de Metricas) coordinados por `docker compose` y respaldados
por Redis. El sistema procesa consultas Q1-Q5 sobre el dataset Google Open
Buildings (subconjunto correspondiente a la Region Metropolitana de Santiago)
precargado en memoria.

---

## Prerrequisitos

| Herramienta | Notas |
|---|---|
| **Docker Engine + Compose v2** | Se invoca como `docker compose` (con espacio). Verifica con `docker compose version`. |
| **Python 3.10+** | Necesario solo para filtrar el dataset y correr los scripts de experimentos. |
| **redis-cli** *(opcional)* | Solo lo usa `experiments/master_run.py` para reconfigurar politicas en runtime. |

---

## Estructura del repositorio

```
.
├── cache_api/                       # Servicio 2 — Cache (Redis)
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py                  # FastAPI: /query, /stats, /flush, /health
│   │   └── cache.py                 # CacheClient con soporte LRU/LFU/FIFO
│   ├── Dockerfile
│   └── requirements.txt
├── generador_respuestas/            # Servicio 3 — Generador de Respuestas
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py                  # FastAPI: /query, /stats, /health
│   │   ├── data_loader.py           # DataStore + zonas + haversine
│   │   └── queries.py               # Q1-Q5 con latencia simulada
│   ├── Dockerfile
│   └── requirements.txt
├── generador_trafico/               # Servicio 1 — Generador de Trafico
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py                  # FastAPI: /run, /stop, /status, /health
│   │   └── distributions.py         # Zipf, Uniforme, Poisson
│   ├── Dockerfile
│   └── requirements.txt
├── metricas/                        # Servicio 4 — Almacenamiento de Metricas
│   ├── app/
│   │   ├── __init__.py
│   │   └── main.py                  # FastAPI: /event, /summary, /snapshot, /reset
│   ├── Dockerfile
│   └── requirements.txt
├── experiments/
│   ├── master_run.py                # Bateria completa (22 corridas)
│   └── build_figures.py             # 7 figuras del informe
├── results/                         # Snapshots JSON de cada experimento
├── data/
│   └── buildings_rm.csv             # Dataset filtrado (generado con filtrar_real.py)
├── filtrar_real.py                  # Script de filtrado del dataset original
├── scripts/
│   └── download_data.py             # Script alternativo de generacion sintetica
├── docker-compose.yml
├── .env                             # Configuracion (politica, tamano, TTLs)
└── README.md
```

---

## Arquitectura (4 servicios + Redis)

| Servicio | Puerto | Rol segun el enunciado |
|---|---|---|
| `generador_trafico` | 5003 | Genera consultas Q1-Q5 con distribuciones Zipf y Uniforme. |
| `cache_api`          | 5000 | Intercepta consultas; sirve hits desde Redis o delega misses. |
| `generador_respuestas`| 5001 | Calcula Q1-Q5 sobre datos precargados en memoria. |
| `metricas`           | 5002 | Registra hits, misses, latencias, throughput y evicciones. |
| `redis` (backing)    | 6379 | Almacen del cache con TTL y politicas de eviccion. |

Las **cinco zonas** (Z1 Providencia, Z2 Las Condes, Z3 Maipu, Z4 Santiago
Centro, Z5 Pudahuel) y los **cinco tipos de consulta** (Q1 conteo, Q2 area,
Q3 densidad, Q4 comparacion, Q5 distribucion de confianza) replican
literalmente la Seccion 4 y 5 del enunciado, con los mismos formatos de
*cache key*.

---

## Despliegue paso a paso

### 1) Preparar el dataset

```bash
mkdir -p data
cp /ruta/al/archivo/967_buildings.csv.gz data/
pip install pandas
python3 filtrar_real.py
```

Esto genera `data/buildings_rm.csv` con las edificaciones de las 5 zonas.

### 2) Levantar el stack

```bash
docker compose up -d --build
```

Espera ~60s a que los healthchecks pasen y verifica:

```bash
curl http://localhost:5003/health
curl http://localhost:5000/health
curl http://localhost:5001/health
curl http://localhost:5002/health
```

Los cuatro deben retornar `{"status":"ok",...}`.

### 3) Probar consultas Q1-Q5 manualmente

Cada consulta se envia al **Cache Service** (puerto 5000):

```bash
# Q1 — conteo en Providencia con confidence_min=0.8
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{"query_type":"Q1","params":{"zone_id":"Z1","confidence_min":0.8}}'

# Q2 — area media y total en Las Condes
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{"query_type":"Q2","params":{"zone_id":"Z2","confidence_min":0.0}}'

# Q3 — densidad en Maipu
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{"query_type":"Q3","params":{"zone_id":"Z3","confidence_min":0.0}}'

# Q4 — comparar densidad Las Condes vs Maipu
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{"query_type":"Q4","params":{"zone_a":"Z2","zone_b":"Z3","confidence_min":0.0}}'

# Q5 — distribucion de confianza en Pudahuel (5 bins)
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{"query_type":"Q5","params":{"zone_id":"Z5","bins":5}}'
```

### 4) Test rapido del pipeline (~1 min)

```bash
pip install numpy matplotlib
python experiments/master_run.py --suite demo
```

### 5) Bateria oficial de experimentos (~20 min)

```bash
python experiments/master_run.py --suite all
```

Esto ejecuta **22 experimentos** = 3 politicas (LRU, LFU, FIFO)
x 3 tamanos (50 MB, 200 MB, 500 MB) x 2 distribuciones (Zipf, Uniforme),
mas 3 corridas adicionales en cache muy pequeno (forzar evicciones) y una
corrida larga de 180s para evidenciar el efecto del TTL.

Snapshots resultantes en `results/snap_*.json`.

### 6) Generar las figuras del informe

```bash
python experiments/build_figures.py
```

Genera 7 figuras en `informe/figs/` (PDF y PNG):

| Figura | Contenido |
|---|---|
| fig1 | Hit rate por distribucion y politica |
| fig2 | Zipf vs Uniforme por politica |
| fig3 | Hit rate por tamano de cache |
| fig4 | Throughput por politica y distribucion |
| fig5 | Latencia hit vs miss (escala log) |
| fig6 | Hit rate por consulta Q1-Q5 |
| fig7 | Cache efficiency por politica y duracion |

---

## Configuracion (`.env`)

```ini
# Tamano maximo de cache (50mb / 200mb / 500mb requeridos por enunciado)
REDIS_MAXMEMORY=200mb

# Politica nativa Redis: allkeys-lru / allkeys-lfu / noeviction (FIFO)
REDIS_POLICY_NATIVE=allkeys-lru
REDIS_PORT_HOST=6379

# Politica expuesta al cache_api: LRU, LFU o FIFO
CACHE_POLICY=LRU

# TTL global por defecto (segundos). 0 = sin expiracion.
CACHE_TTL_SEC=300

# TTL especificos por consulta
TTL_Q1=300
TTL_Q2=300
TTL_Q3=180
TTL_Q4=120
TTL_Q5=600

# Latencia simulada del Generador de Respuestas (computo geoespacial)
SIM_LATENCY_MIN_MS=30
SIM_LATENCY_MAX_MS=120
```

`experiments/master_run.py` reconfigura `maxmemory-policy` y `maxmemory` en
runtime con `docker compose exec redis redis-cli CONFIG SET`, evitando
reiniciar contenedores entre combinaciones.

---

## API HTTP

### Generador de Trafico (5003)

| Metodo | Ruta | Descripcion |
|---|---|---|
| POST | `/run` | Inicia un experimento |
| GET  | `/status` | Progreso del experimento actual |
| POST | `/stop` | Detiene experimento en curso |
| GET  | `/health` | Healthcheck |

Body de `/run`:

```json
{
  "distribution": "zipf",
  "rate_qps": 60,
  "duration_sec": 30,
  "zipf_s": 1.5,
  "concurrency": 16,
  "seed": 42,
  "label": "mi_experimento"
}
```

### Cache API (5000)

| Metodo | Ruta | Descripcion |
|---|---|---|
| POST | `/query` | Consulta Q1-Q5 (entrada del pipeline) |
| GET  | `/stats` | Stats agregados de Redis |
| POST | `/flush` | Limpia el cache |
| GET  | `/health` | Healthcheck |

### Generador de Respuestas (5001)

| Metodo | Ruta | Descripcion |
|---|---|---|
| POST | `/query` | Ejecuta Q1-Q5 sobre datos en memoria |
| GET  | `/stats` | Edificaciones cargadas por zona |
| GET  | `/health` | Healthcheck |

### Servicio de Metricas (5002)

| Metodo | Ruta | Descripcion |
|---|---|---|
| POST | `/event` | Recibe eventos hit / miss / error |
| GET  | `/summary` | Hit rate, throughput, p50/p95, eviction rate, cache efficiency |
| GET  | `/summary/by_query` | Desglose por Q1-Q5 |
| POST | `/snapshot` | Persiste snapshot a disco |
| POST | `/reset` | Reinicia metricas |
| GET  | `/health` | Healthcheck |

---

## Consultas disponibles (Q1-Q5)

| Consulta | Descripcion | Parametros |
|---|---|---|
| **Q1** | Conteo de edificios en una zona | `zone_id`, `confidence_min` |
| **Q2** | Area promedio y total de edificaciones | `zone_id`, `confidence_min` |
| **Q3** | Densidad de edificaciones por km2 | `zone_id`, `confidence_min` |
| **Q4** | Comparacion de densidad entre dos zonas | `zone_a`, `zone_b`, `confidence_min` |
| **Q5** | Distribucion de confianza en bins | `zone_id`, `bins` |

---

## Mapeo con el enunciado

| Requisito (PDF) | Implementacion |
|---|---|
| 4 servicios independientes | `generador_trafico`, `cache_api`, `generador_respuestas`, `metricas` |
| Cache Redis con TTL y eviccion configurable | `redis:7.4` + `cache_api` (LRU/LFU nativos, FIFO en cliente) |
| Distribuciones Zipf y Uniforme | `generador_trafico/app/distributions.py` |
| Q1-Q5 sobre datos precargados | `generador_respuestas/app/queries.py` |
| Cache keys exactos del enunciado | `cache_api/app/main.py:_build_cache_key` |
| Cinco zonas con bounding boxes | `generador_respuestas/app/data_loader.py:ZONES` |
| Tamanos 50/200/500 MB | `experiments/master_run.py:SIZES` |
| Hit rate / throughput / p50/p95 / eviction rate / cache efficiency | `metricas/app/main.py:Metrics.summary` |
| Analisis comparativo y figuras | `experiments/build_figures.py` + `informe/` |
| Despliegue Docker | `docker-compose.yml` (Compose v2) |

---

## Troubleshooting

**`docker compose up` falla con "dataset no encontrado"**

Verifica que `data/buildings_rm.csv` existe. Si no, ejecuta `python3 filtrar_real.py`.

**El cache service reporta OOM o falla al iniciar**

Aumenta `REDIS_MAXMEMORY` en `.env` (minimo recomendado `50mb`) y reinicia:

```bash
docker compose restart redis cache_api
```

**Cambiar politica sin reiniciar todo el stack**

```bash
docker compose exec redis redis-cli CONFIG SET maxmemory-policy allkeys-lfu
docker compose exec redis redis-cli FLUSHDB
```

**Apagar y limpiar todo**

```bash
docker compose down -v
```

