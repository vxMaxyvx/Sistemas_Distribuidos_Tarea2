# Tarea 1 - Sistemas Distribuidos 2026-1

**Integrantes:** Vicente Cataldo, Maximiliano Oliva

## Descripcion del proyecto

Sistema distribuido para el analisis de datos geoespaciales de edificaciones en la Region Metropolitana de Santiago. El sistema procesa un dataset de Google Open Buildings (archivo `967_buildings.csv.gz`) para responder consultas Q1-Q5 sobre 5 zonas predefinidas de Santiago. Todo el procesamiento se hace en memoria RAM y se utiliza Redis como backend de cache con politicas de eviccion configurables (LRU, LFU, FIFO).

## Estructura de carpetas

```
.
├── cache_api/              # API intermediaria de cache (Redis + delegacion)
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── generador_respuestas/   # Procesador de consultas Q1-Q5 en memoria
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── generador_trafico/      # Generador de consultas sinteticas (Zipf / Uniforme)
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── metricas/               # Almacenamiento de metricas en memoria RAM
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── data/                   # Dataset de edificaciones
│   ├── 967_buildings.csv.gz   # <-- archivo original (NO subir a git)
│  
├── filtrar_real.py         # Script de filtrado del dataset original
├── scripts/
│   └── download_data.py    # Script alternativo de generacion sintetica
├── docker-compose.yml
└── README.md
```

## Requisitos previos

- Docker y Docker Compose instalados
- Python 3.11+ con `pandas` instalado (solo para ejecutar `filtrar_real.py` localmente)
- El archivo `967_buildings.csv.gz` proporcionado por el curso

## Paso 1: Preparar el dataset

1. Crea la carpeta `data/` si no existe:
   ```bash
   mkdir -p data
   ```

2. Copia el archivo `967_buildings.csv.gz` (proporcionado por el curso) dentro de `data/`:
   ```bash
   cp /ruta/al/archivo/967_buildings.csv.gz data/
   ```

## Paso 2: Compilar y ejecutar el filtrado del dataset

El script `filtrar_real.py` lee el archivo `data/967_buildings.csv.gz` en pedazos de 1 millon de filas para no explotar la RAM, filtra las edificaciones que pertenecen a las 5 zonas de Santiago, y genera `data/buildings_rm.csv`.

```bash
# Instalar pandas si no lo tienes
pip install pandas

# Ejecutar el filtrado
python3 filtrar_real.py
```

Este archivo `buildings_rm.csv` es el que usara el servicio `generador_respuestas` para responder las consultas.

## Paso 3: Levantar los servicios base

Los 4 servicios principales + Redis se levantan con un solo comando:

```bash
docker compose up --build -d redis generador_respuestas metricas cache_api
```

Servicios que se levantan:
- `redis` — Motor de cache en memoria (puerto host: 6380)
- `generador_respuestas` — Procesa Q1-Q5 con datos en RAM (puerto host: 5002)
- `metricas` — Almacena hits/misses/latencias en RAM (puerto host: 5004)
- `cache_api` — Intermediaria entre trafico y generador, usa Redis (puerto host: 5003)

Espera unos segundos a que `generador_respuestas` cargue el CSV en memoria. Puedes verificar que todo este OK:

```bash
curl http://localhost:5002/health
curl http://localhost:5003/health
curl http://localhost:5004/health
```

## Paso 4: Ejecutar trafico con distribucion Zipf y distribucion Uniforme

El generador de trafico simula consultas con distribucion Zipf (ley de potencia), donde algunas consultas son mucho mas frecuentes que otras. Esto favorece el cache.

Por defecto genera **100.000 consultas** a **1000 qps**. Puedes modificarlo con variables de entorno:


Tamaño 50MB

LRU:
```bash
REDIS_MAX_MEMORY="50mb" REDIS_EVICTION_POLICY="allkeys-lru" docker compose --profile zipf up trafico_zipf
```

LFU:
```bash
REDIS_MAX_MEMORY="50mb" REDIS_EVICTION_POLICY="allkeys-lfu" docker compose --profile zipf up trafico_zipf   
```

Tamaño 200MB

LRU:
```bash
REDIS_MAX_MEMORY="200mb" REDIS_EVICTION_POLICY="allkeys-lru" docker compose --profile zipf up trafico_zipf
```

LFU:
```bash
REDIS_MAX_MEMORY="200mb" REDIS_EVICTION_POLICY="allkeys-lfu" docker compose --profile zipf up trafico_zipf
```



Tamaño 500MB

LRU:
```bash
REDIS_MAX_MEMORY="500mb" REDIS_EVICTION_POLICY="allkeys-lru" docker compose --profile zipf up trafico_zipf
```

LFU:
```bash
REDIS_MAX_MEMORY="500mb" REDIS_EVICTION_POLICY="allkeys-lfu" docker compose --profile zipf up trafico_zipf
```



Para comparar, ejecuta el mismo trafico pero con distribucion uniforme (todas las consultas tienen la misma probabilidad). Esto produce menos cache hits.

Tamaño 50MB

LRU:
```bash
REDIS_MAX_MEMORY="50mb" REDIS_EVICTION_POLICY="allkeys-lru" docker compose --profile uniform up trafico_uniform
```

LFU:
```bash
REDIS_MAX_MEMORY="50mb" REDIS_EVICTION_POLICY="allkeys-lfu" docker compose --profile uniform up trafico_uniform
```

Tamaño 200MB

LRU:
```bash
REDIS_MAX_MEMORY="200mb" REDIS_EVICTION_POLICY="allkeys-lru" docker compose --profile uniform up trafico_uniform
```

LFU:
```bash
REDIS_MAX_MEMORY="200mb" REDIS_EVICTION_POLICY="allkeys-lfu" docker compose --profile uniform up trafico_uniform
```

Tamaño 500MB

LRU:
```bash
REDIS_MAX_MEMORY="500mb" REDIS_EVICTION_POLICY="allkeys-lru" docker compose --profile uniform up trafico_uniform
```

LFU
```bash
REDIS_MAX_MEMORY="500mb" REDIS_EVICTION_POLICY="allkeys-lfu" docker compose --profile uniform up trafico_uniform
```

## Paso 6: Consultar metricas del sistema

Despues de ejecutar los generadores de trafico, consulta las metricas acumuladas:

### Metricas agregadas (hit rate, latencia, throughput)
```bash
curl http://localhost:5004/stats | python3 -m json.tool
```

### Desglose por tipo de consulta (Q1-Q5)
```bash
curl http://localhost:5004/query_breakdown | python3 -m json.tool
```

### Estadisticas del cache Redis
```bash
curl http://localhost:5003/stats | python3 -m json.tool
```

### Exportar metricas a CSV (para analisis en Excel/Python)
```bash
curl http://localhost:5004/export/csv > metricas.csv
```

### Exportar metricas a JSON
```bash
curl http://localhost:5004/export/json > metricas.json
```

### Limpiar el cache (para reiniciar experimentos)
```bash
curl -X POST http://localhost:5003/flush
```

## Configuracion avanzada del cache

Las siguientes variables de entorno afectan el comportamiento del sistema:

| Variable | Descripcion | Default |
|---|---|---|
| `REDIS_MAX_MEMORY` | Memoria maxima de Redis | `200mb` |
| `REDIS_EVICTION_POLICY` | Politica de eviccion | `allkeys-lru` |
| `CACHE_TTL` | Tiempo de vida de una key en cache (segundos) | `60` |
| `TOTAL_QUERIES` | Cantidad total de consultas a generar | Zipf: `100000`, Uniforme: `30000` |
| `QPS` | Consultas por segundo | `1000` |


## Consultas disponibles (Q1-Q5)

| Consulta | Descripcion | Parametros |
|---|---|---|
| **Q1** | Conteo de edificios en una zona | `zone_id`, `confidence_min` |
| **Q2** | Area promedio y total de edificaciones | `zone_id`, `confidence_min` |
| **Q3** | Densidad de edificaciones por km² | `zone_id`, `confidence_min` |
| **Q4** | Comparacion de densidad entre dos zonas | `zone_a`, `zone_b`, `confidence_min` |
| **Q5** | Distribucion de confianza en bins | `zone_id`, `bins` |


## Apagar el sistema

```bash
docker compose down -v
```

