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
