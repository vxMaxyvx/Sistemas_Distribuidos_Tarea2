# Tarea 2 — Sistemas Distribuidos 2026-1
### Plataforma de análisis de consultas geoespaciales asíncrona con Apache Kafka y Caché

**Integrantes:** Vicente Cataldo, Maximiliano Oliva  
**Stack:** Python 3.12, FastAPI, Apache Kafka (con Zookeeper), Redis 7.4, Docker Compose v2

Este repositorio implementa la **Tarea 2** de Sistemas Distribuidos, la cual evoluciona la arquitectura síncrona original incorporando **Apache Kafka** como un sistema de mensajería asíncrono y tolerante a fallos. El sistema procesa consultas geoespaciales (Q1-Q5) de forma desacoplada, implementando colas de reintentos, colas de descarte (Dead Letter Queue - DLQ), escalamiento de consumidores y monitorización de métricas avanzadas.

---

## Características de la Arquitectura (Tarea 2)

*   **Desacoplamiento con Kafka**: El Generador de Tráfico (productor) publica consultas en el tópico `queries` sin bloquearse esperando respuestas.
*   **Consumidor Escalable (`cache_api`)**: El servicio de caché actúa como consumidor de Kafka, permitiendo su escalamiento horizontal. Consume mensajes, consulta la caché (Redis), redirige los misses al Generador de Respuestas mediante HTTP y publica las respuestas.
*   **Mecanismo de Reintentos y DLQ**:
    *   Si el Generador de Respuestas falla temporalmente (retorna HTTP 5xx), el mensaje es reenviado al tópico de reintentos `retry-queries` con retraso exponencial (backoff).
    *   Si supera el máximo de intentos (`MAX_RETRIES`), el mensaje se descarta enviándose al tópico `dlq-queries` (Dead Letter Queue) para evitar la pérdida de consultas.
*   **Modo Asíncrono / Síncrono Flexible**: Controlado mediante la variable `USE_KAFKA` en el archivo `.env`.
*   **Simulador de Fallas**: Endpoint `/toggle_failure` en el Generador de Respuestas para simular caídas temporales de red o de cómputo y evaluar la resiliencia del sistema.
*   **Monitorización en Tiempo Real**: El servicio de métricas ahora mide el tamaño del backlog en Kafka (lag), la tasa de reintentos (`retry_rate`), tasa de recuperación (`recovery_rate`) y tasa de descarte (`dlq_rate`).

---

## Estructura del Repositorio

```
.
├── cache_api/                       # Servicio 2 — Cache & Kafka Consumer
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py                  # FastAPI: Consumer loop, /stats, /flush, /health
│   │   └── cache.py                 # Cliente de Caché con políticas LRU/LFU/FIFO
│   ├── Dockerfile
│   └── requirements.txt
├── generador_respuestas/            # Servicio 3 — Generador de Respuestas
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py                  # FastAPI: /query, /toggle_failure (simulación), /health
│   │   ├── data_loader.py           # Carga en memoria de edificaciones
│   │   └── queries.py               # Algoritmos Q1-Q5 con latencia simulada
│   ├── Dockerfile
│   └── requirements.txt
├── generador_trafico/               # Servicio 1 — Generador de Trafico & Producer
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py                  # FastAPI: /run (publica a Kafka o HTTP), /status, /health
│   │   └── distributions.py         # Distribuciones Zipf, Uniforme, Poisson
│   ├── Dockerfile
│   └── requirements.txt
├── metricas/                        # Servicio 4 — Almacenamiento de Métricas
│   ├── app/
│   │   ├── __init__.py
│   │   └── main.py                  # FastAPI: /event, /summary (incluye lag Kafka), /snapshot
│   ├── Dockerfile
│   └── requirements.txt
├── experiments/
│   ├── run_kafka_experiments.py     # Script para ejecutar la batería de 8 experimentos
│   └── build_kafka_figures.py       # Generador de los 8 gráficos del informe
├── data/
│   └── buildings_rm.csv             # Dataset de Santiago Metropolitana filtrado
├── filtrar_real.py                  # Filtra el CSV original de Google Open Buildings
├── docker-compose.yml               # Orquestación multi-contenedor (incluye Kafka)
├── .env                             # Variables de configuración del sistema
└── README.md                        # Documentación general
```

---

## Arquitectura de Red (5 servicios + Middleware)

| Servicio | Puerto Host | Rol / Descripción |
|---|---|---|
| `generador_trafico` | 5003 | **Productor / HTTP API**: Publica consultas en tópicos Kafka o envía HTTP según `USE_KAFKA`. |
| `cache_api`          | 5000 | **Consumidor / API de Caché**: Escala horizontalmente, consume de Kafka y consulta Redis. |
| `generador_respuestas`| 5001 | **Cómputo**: Resuelve consultas geoespaciales y expone endpoint `/toggle_failure`. |
| `metricas`           | 5002 | **Métricas**: Registra eventos del pipeline, calcula percentiles y obtiene lag de Kafka. |
| `redis`              | 6379 | **Almacenamiento**: Caché rápida en memoria (LRU/LFU/FIFO). |
| `zookeeper`          | 2181 | **Coordinador**: Administra el clúster de Apache Kafka. |
| `kafka`              | 9092 | **Message Broker**: Canaliza tópicos `queries`, `retry-queries` y `dlq-queries`. |

---

## Despliegue y Ejecución

### 1) Configuración de Datos
Asegúrate de contar con el dataset filtrado `data/buildings_rm.csv`. Si no lo tienes y cuentas con el archivo original comprimido `967_buildings.csv.gz`, ejecuta:
```bash
mkdir -p data
cp /ruta/al/archivo/967_buildings.csv.gz data/
pip install pandas
python3 filtrar_real.py
```

### 2) Levantar el Entorno Distribuidos
El archivo `.env` está configurado por defecto con `USE_KAFKA=true`. Levanta el clúster con:
```bash
docker compose up -d --build
```
*Nota: Se ha configurado un healthcheck detallado para Kafka. Los servicios dependientes esperarán automáticamente a que el broker esté 100% operativo antes de iniciar.*

### 3) Escalamiento de Consumidores
Para evaluar el impacto del escalamiento horizontal, puedes ajustar dinámicamente la cantidad de consumidores corriendo el siguiente comando:
```bash
docker compose up -d --scale cache_api=3
```
Esto creará 3 instancias de `cache_api` consumiendo en paralelo del tópico de Kafka.

### 4) Ejecutar Batería de Experimentos Evaluativos
El script automatizado ejecuta de manera secuencial los **8 escenarios experimentales** descritos en el enunciado:
```bash
pip install numpy matplotlib
python experiments/run_kafka_experiments.py
```
Este proceso reinicia los contenedores, escala las instancias de acuerdo con el escenario y toma snapshots de métricas en la carpeta `results/`.

### 5) Generar las Figuras del Informe
Una vez recolectados los resultados de los experimentos, puedes generar las **8 figuras comparativas de alta calidad** corriendo:
```bash
python experiments/build_kafka_figures.py
```
Los gráficos se exportarán en formato `PDF` y `PNG` dentro del directorio `informe/figs/`:
*   **`fig1_throughput_comparison`**: Throughput del sistema síncrono vs asíncrono.
*   **`fig2_latency_comparison`**: Percentiles de latencias (p50/p95) en escala logarítmica.
*   **`fig3_reliability_comparison`**: Consultas completadas vs perdidas ante caídas temporales.
*   **`fig4_backlog_evolution`**: Evolución del lag y tiempo de recuperación posterior a fallas.
*   **`fig5_retry_dlq_rates`**: Tasa de reintentos y desvíos a DLQ según cantidad de consumidores.
*   **`fig6_spike_backlog`**: Acumulación de lag en Kafka ante ráfagas repentinas (spikes) de tráfico.
*   **`fig7_scaling_consumers`**: Comparativa de escalabilidad (1 vs 3 vs 5 consumidores).
*   **`fig8_distribution_comparison`**: Comparativa de hit rate y latencia mediana de Zipf vs Uniforme.

---

## Configuración y Variables de Entorno (`.env`)

El archivo `.env` en la raíz contiene las siguientes opciones críticas para la Tarea 2:

```ini
# --- Configuración de Caché (Redis) ---
REDIS_MAXMEMORY=200mb
REDIS_POLICY_NATIVE=allkeys-lru
CACHE_POLICY=LRU
CACHE_TTL_SEC=300

# --- Modo de Operación Kafka (Tarea 2) ---
# true  = Utiliza colas asíncronas de Kafka (Tarea 2)
# false = Utiliza llamadas síncronas HTTP directas (Tarea 1)
USE_KAFKA=true

# --- Resiliencia y Tolerancia a Fallos ---
# Número máximo de intentos antes de enviar la consulta al tópico DLQ
MAX_RETRIES=3
```

---

## Endpoints HTTP Clave para Desarrollo

### Generador de Respuestas (5001)
*   `POST /query`: Resuelve la consulta de manera síncrona.
*   `POST /toggle_failure`: Recibe un JSON `{"enabled": true|false}` para simular la caída del componente de cálculo y forzar reintentos/DLQ.

### Servicio de Métricas (5002)
*   `GET /summary`: Obtiene un JSON con el rendimiento general de latencias, throughputs y el **lag acumulado en Kafka**.
*   `POST /snapshot`: Captura el estado actual de las métricas etiquetado bajo el nombre de un escenario.
*   `POST /reset`: Limpia los acumuladores de estadísticas.
