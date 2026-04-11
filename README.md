# Tarea 1 - Sistemas Distribuidos 2026-1

**Integrantes:** Maximiliano Oliva, Vicente Cataldo  
**Universidad:** UDP  
**Curso:** Sistemas Distribuidos

## Descripcion

Sistema distribuido para el analisis de datos geoespaciales de edificaciones en la Region Metropolitana de Santiago, utilizando el dataset Google Open Buildings. El sistema implementa un pipeline con cache basado en Redis para optimizar consultas recurrentes sobre zonas predefinidas.

## Arquitectura

El sistema se compone de 4 servicios principales:

1. **Generador de Trafico** — Genera consultas sinteticas con distribuciones Zipf y Uniforme
2. **Cache (Redis)** — Intercepta consultas, retorna desde cache o delega al generador de respuestas
3. **Generador de Respuestas** — Procesa consultas Q1-Q5 sobre datos precargados en memoria
4. **Almacenamiento de Metricas (PostgreSQL)** — Registra hits, misses, latencias y evictions

## Requisitos

- Docker y Docker Compose

## Despliegue

### 1. Clonar el repositorio

```bash
git clone <url-del-repo>
cd Sistemas_Distribuidos_Tarea1
```

### 2. Generar el dataset (si no existe)

```bash
python3 scripts/download_data.py
```

### 3. Levantar los servicios base

```bash
docker compose up --build -d postgres redis generador_respuestas cache
```

### 4. Ejecutar trafico con distribucion Zipf

```bash
docker compose --profile zipf up trafico_zipf
```

### 5. Ejecutar trafico con distribucion Uniforme

```bash
docker compose --profile uniform up trafico_uniform
```

### 6. Ejecutar ambos perfiles

```bash
docker compose --profile all up
```

## Configuracion

Se pueden ajustar parametros mediante variables de entorno antes de ejecutar:

| Variable | Descripcion | Default |
|---|---|---|
| `REDIS_MAX_MEMORY` | Memoria maxima de Redis | `200mb` |
| `REDIS_EVICTION_POLICY` | Politica de eviccion | `allkeys-lru` |
| `CACHE_TTL` | Tiempo de vida del cache (seg) | `60` |
| `TOTAL_QUERIES` | Cantidad de consultas a generar | `1000` |
| `QPS` | Consultas por segundo | `10` |

### Ejemplo: Cambiar politica a LFU con 50MB

```bash
REDIS_MAX_MEMORY=50mb REDIS_EVICTION_POLICY=allkeys-lfu docker compose up --build -d
```

## Consultas de metricas

### Cache stats

```bash
curl http://localhost:5003/stats
```

### Consultas en PostgreSQL

```bash
docker exec -it tarea1_postgres psql -U admin -d metricas -c "
  SELECT cache_hit, COUNT(*), ROUND(AVG(latency_ms)::numeric, 2) as avg_latency
  FROM query_metrics GROUP BY cache_hit;
"
```

## Estructura del proyecto

```
.
├── cache/                    # Servicio de cache
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── generador_respuestas/     # Procesador de consultas Q1-Q5
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── generador_trafico/        # Generador de consultas sinteticas
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── db/                       # Esquema SQL
│   └── init.sql
├── data/                     # Dataset de edificaciones
│   └── buildings_rm.csv
├── scripts/                  # Scripts auxiliares
│   └── download_data.py
├── docker-compose.yml
└── README.md
```

## Zonas de consulta

| Zona | ID | lat_min | lat_max | lon_min | lon_max |
|---|---|---|---|---|---|
| Providencia | Z1 | -33.445 | -33.420 | -70.640 | -70.600 |
| Las Condes | Z2 | -33.420 | -33.390 | -70.600 | -70.550 |
| Maipu | Z3 | -33.530 | -33.490 | -70.790 | -70.740 |
| Santiago Centro | Z4 | -33.460 | -33.430 | -70.670 | -70.630 |
| Pudahuel | Z5 | -33.470 | -33.430 | -70.810 | -70.760 |

## Tipos de consultas

- **Q1** — Conteo de edificios en una zona
- **Q2** — Area promedio y total de edificaciones
- **Q3** — Densidad de edificaciones por km²
- **Q4** — Comparacion de densidad entre dos zonas
- **Q5** — Distribucion de confianza en una zona
