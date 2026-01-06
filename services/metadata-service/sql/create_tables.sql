CREATE TABLE IF NOT EXISTS schemas (
    id SERIAL PRIMARY KEY,
    project_name TEXT NOT NULL,
    name TEXT NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (project_name, name, version)
);

CREATE TABLE IF NOT EXISTS avro_schemas (
    id SERIAL PRIMARY KEY,
    schema_id INT NOT NULL REFERENCES schemas(id) ON DELETE CASCADE,
    definition TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS parquet_schemas (
    id SERIAL PRIMARY KEY,
    schema_id INT NOT NULL REFERENCES schemas(id) ON DELETE CASCADE,
    definition TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS history_schemas (
    id SERIAL PRIMARY KEY,
    project_name VARCHAR(100) NOT NULL,
    name VARCHAR(100) NOT NULL,
    fields JSONB NOT NULL,
    version INT NOT NULL
);

CREATE TABLE IF NOT EXISTS project (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    owner_id UUID NOT NULL
);

CREATE TABLE IF NOT EXISTS data_files (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    schema_id INT NOT NULL REFERENCES schemas(id) ON DELETE CASCADE,
    block_id UUID NOT NULL UNIQUE,
    worker_id UUID NOT NULL,
    path TEXT NOT NULL,
    size BIGINT NOT NULL,
    format TEXT NOT NULL,
    is_compacted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS compaction_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    schema_id INT NOT NULL REFERENCES schemas(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    target_block_ids TEXT[] NOT NULL, -- Array of UUID strings
    output_file_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);