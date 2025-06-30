CREATE TABLE IF NOT EXISTS etl_configs (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    config_data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT etl_configs_name_unique UNIQUE (name)
);

COMMENT ON TABLE etl_configs IS 'Almacena configuraciones de ETL para reutilización';
COMMENT ON COLUMN etl_configs.name IS 'Nombre único de la configuración';
COMMENT ON COLUMN etl_configs.description IS 'Descripción de la configuración';
COMMENT ON COLUMN etl_configs.config_data IS 'Configuración en formato JSON (mapeo, transformaciones, etc.)';
