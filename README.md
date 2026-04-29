# Tarea 1 - Sistemas Distribuidos 2026-1

**Integrantes:** Maximiliano Oliva, Vicente Cataldo  
**Universidad:** UDP  
**Curso:** Sistemas Distribuidos

## Descripcion

Sistema distribuido para el analisis de datos geoespaciales de edificaciones en la Region Metropolitana de Santiago, utilizando el dataset Google Open Buildings. El sistema implementa un pipeline con cache basado en Redis para optimizar consultas recurrentes sobre zonas predefinidas. Todos los datos se procesan en memoria RAM, sin dependencia de bases de datos externas.

## Arquitectura

El sistema se compone de 4 servicios principales + Redis como backend de cache:

1. **Generador de Trafico** — Genera consultas sinteticas con distribuciones Zipf y Uniforme
2. **Cache API** — Intercepta consultas, usa Redis como backend de cache; delega al generador de respuestas en caso de miss
3. **Redis** — Motor de cache en memoria con TTL y politicas de eviccion configurables (LRU, LFU, FIFO)
4. **Generador de Respuestas** — Carga el dataset CSV en memoria (RAM) y procesa consultas Q1-Q5 directamente
5. **Metricas** — Recibe eventos del sistema via HTTP y los almacena en memoria (RAM); provee endpoints para analisis

## Requisitos

- Docker y Docker Compose

## Despliegue

### 1. Clonar el repositorio

```bash
git clone <url-del-repo>
cd Sistemas_Distribuidos_Tarea1
```

### 2. Generar el dataset

```bash
python3 scripts/download_data.py
```

### 3. Levantar los 4 servicios base + Redis

```bash
docker compose up --build -d redis generador_respuestas metricas cache_api
```

### 4. Ejecutar trafico con distribucion Zipf

```bash
docker compose --profile zipf up trafico_zipf
```

### 5. Ejecutar trafico con distribucion Uniforme

```bash
docker compose --profile uniform up trafico_uniform
```

### 6. Ejecutar ambos perfiles simultaneamente

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

### Cache stats (Redis)

```bash
curl http://localhost:5003/stats
```

### Metricas agregadas del sistema

```bash
curl http://localhost:5004/stats
```

### Desglose por tipo de consulta

```bash
curl http://localhost:5004/query_breakdown
```

### Exportar metricas a CSV

```bash
curl http://localhost:5004/export/csv > metricas.csv
```

### Exportar metricas a JSON

```bash
curl http://localhost:5004/export/json > metricas.json
```

## Estructura del proyecto

```
.
├── cache_api/                # API intermediaria de cache (Redis + delegacion)
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── generador_respuestas/     # Procesador de consultas Q1-Q5 (datos en memoria)
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── generador_trafico/        # Generador de consultas sinteticas (Zipf / Uniforme)
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── metricas/                 # Almacenamiento de metricas en memoria RAM
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── scripts/                  # Script para generar dataset sintetico
│   └── download_data.py
├── data/                     # Dataset de edificaciones
│   └── buildings_rm.csv
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
