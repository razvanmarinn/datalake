CREATE TABLE IF NOT EXISTS schemas (
    id SERIAL PRIMARY KEY,
    project_name VARCHAR(100) NOT NULL,
    name VARCHAR(100) NOT NULL,
    fields JSONB NOT NULL,
    version INT NOT NULL
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
    owner_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE
)