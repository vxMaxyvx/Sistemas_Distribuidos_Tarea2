-- Esquema de base de datos para el almacenamiento de metricas

CREATE TABLE IF NOT EXISTS query_metrics (
    id SERIAL PRIMARY KEY,
    query_type VARCHAR(10) NOT NULL,
    cache_key VARCHAR(256) NOT NULL,
    cache_hit BOOLEAN NOT NULL,
    latency_ms FLOAT NOT NULL,
    source VARCHAR(20) NOT NULL,  -- 'cache', 'db', 'error'
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS eviction_metrics (
    id SERIAL PRIMARY KEY,
    evicted_keys BIGINT NOT NULL,
    recorded_at TIMESTAMP DEFAULT NOW()
);

-- Indices para consultas de analisis
CREATE INDEX idx_metrics_query_type ON query_metrics(query_type);
CREATE INDEX idx_metrics_cache_hit ON query_metrics(cache_hit);
CREATE INDEX idx_metrics_created_at ON query_metrics(created_at);
CREATE INDEX idx_eviction_recorded ON eviction_metrics(recorded_at);
