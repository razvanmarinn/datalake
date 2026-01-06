package db

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"go.uber.org/zap"

	"github.com/razvanmarinn/metadata-service/internal/models"

	_ "github.com/lib/pq"
)

// ... [GetDBConfig and getEnv remain unchanged] ...

func GetDBConfig() (string, int, string, string, string) {
	host := getEnv("DB_HOST", "localhost")
	portStr := getEnv("DB_PORT", "5432")
	port := 5432
	if portStr != "" {
		fmt.Sscanf(portStr, "%d", &port)
	}
	user := getEnv("DB_USER", "identityuser")
	password := getEnv("DB_PASSWORD", "identitypassword")
	dbname := getEnv("DB_NAME", "postgres")

	return host, port, user, password, dbname
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func Connect_to_db(logger *logging.Logger) (*sql.DB, error) {
	// ... [Connection logic remains unchanged] ...
    // Note: Ensure your 'sql/create_tables.sql' contains the new table definitions above!
	host, port, user, password, dbname := GetDBConfig()

	defaultConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		host, port, user, password)

	logger.Info("Connecting to default database", zap.String("host", host), zap.Int("port", port))
	defaultDB, err := sql.Open("postgres", defaultConnStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to default database: %v", err)
	}
	defer defaultDB.Close()

	if err = defaultDB.Ping(); err != nil {
		return nil, fmt.Errorf("could not ping default database: %v", err)
	}

	var exists bool
	query := fmt.Sprintf("SELECT EXISTS(SELECT datname FROM pg_database WHERE datname = '%s')", dbname)
	err = defaultDB.QueryRow(query).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("error checking database existence: %v", err)
	}

	if !exists {
		_, err = defaultDB.Exec(fmt.Sprintf("CREATE DATABASE %s", dbname))
		if err != nil {
			return nil, fmt.Errorf("error creating database: %v", err)
		}
		log.Printf("Database %s created successfully", dbname)
	}

	dbConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to target database: %v", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("could not ping target database: %v", err)
	}

    // IMPORTANT: This SQL file must create the schemas, avro_schemas, and parquet_schemas tables
	sqlFilePath := getEnv("SQL_PATH", "./sql/create_tables.sql")
	sqlBytes, err := ioutil.ReadFile(sqlFilePath)
	if err != nil {
		return nil, fmt.Errorf("error reading SQL file from '%s': %v", sqlFilePath, err)
	}

	_, err = db.Exec(string(sqlBytes))
	if err != nil {
		return nil, fmt.Errorf("error executing SQL file: %v", err)
	}

	log.Println("Database tables initialized successfully")
	return db, nil
}

// CreateSchema inserts Version 1 of a new schema into all 3 tables
func CreateSchema(db *sql.DB, schema models.SchemaWithDetails) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// 1. Insert Metadata (Version 1)
	var schemaID int
	err = tx.QueryRow(
		"INSERT INTO schemas (project_name, name, version) VALUES ($1, $2, 1) RETURNING id",
		schema.ProjectName,
		schema.Name,
	).Scan(&schemaID)
	if err != nil {
		return fmt.Errorf("error inserting schema metadata: %v", err)
	}

	// 2. Insert Avro Definition
	_, err = tx.Exec(
		"INSERT INTO avro_schemas (schema_id, definition) VALUES ($1, $2)",
		schemaID, schema.AvroSchema,
	)
	if err != nil {
		return fmt.Errorf("error inserting avro definition: %v", err)
	}

	// 3. Insert Parquet Definition
	_, err = tx.Exec(
		"INSERT INTO parquet_schemas (schema_id, definition) VALUES ($1, $2)",
		schemaID, schema.ParquetSchema,
	)
	if err != nil {
		return fmt.Errorf("error inserting parquet definition: %v", err)
	}

	return tx.Commit()
}

// GetSchema joins the three tables to return the LATEST version
func GetSchema(db *sql.DB, projectName, schemaName string) (*models.SchemaWithDetails, error) {
	query := `
		SELECT 
			s.id, s.project_name, s.name, s.version, 
			a.definition as avro_def, 
			p.definition as parquet_def
		FROM schemas s
		JOIN avro_schemas a ON s.id = a.schema_id
		JOIN parquet_schemas p ON s.id = p.schema_id
		WHERE s.project_name = $1 AND s.name = $2
		ORDER BY s.version DESC 
		LIMIT 1`

	var s models.SchemaWithDetails
	err := db.QueryRow(query, projectName, schemaName).Scan(
		&s.ID, &s.ProjectName, &s.Name, &s.Version, &s.AvroSchema, &s.ParquetSchema,
	)

	if err != nil {
		return nil, err // Let the caller handle sql.ErrNoRows
	}

	return &s, nil
}

// UpdateSchema increments the version and inserts a new row
func UpdateSchema(db *sql.DB, schema models.SchemaWithDetails) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// 1. Find the current max version
	var currentVersion int
	err = tx.QueryRow(
		"SELECT COALESCE(MAX(version), 0) FROM schemas WHERE project_name = $1 AND name = $2",
		schema.ProjectName, schema.Name,
	).Scan(&currentVersion)
	if err != nil {
		return fmt.Errorf("error checking current version: %v", err)
	}

	if currentVersion == 0 {
		return fmt.Errorf("cannot update: schema does not exist")
	}

	// 2. Insert New Version Metadata
	newVersion := currentVersion + 1
	var newID int
	err = tx.QueryRow(
		"INSERT INTO schemas (project_name, name, version) VALUES ($1, $2, $3) RETURNING id",
		schema.ProjectName, schema.Name, newVersion,
	).Scan(&newID)
	if err != nil {
		return fmt.Errorf("error inserting new version: %v", err)
	}

	// 3. Insert New Avro Definition
	_, err = tx.Exec(
		"INSERT INTO avro_schemas (schema_id, definition) VALUES ($1, $2)",
		newID, schema.AvroSchema,
	)
	if err != nil {
		return fmt.Errorf("error inserting new avro definition: %v", err)
	}

	// 4. Insert New Parquet Definition
	_, err = tx.Exec(
		"INSERT INTO parquet_schemas (schema_id, definition) VALUES ($1, $2)",
		newID, schema.ParquetSchema,
	)
	if err != nil {
		return fmt.Errorf("error inserting new parquet definition: %v", err)
	}

	return tx.Commit()
}

// ListSchemas returns the latest version of every schema in a project
func ListSchemas(db *sql.DB, projectName string) ([]models.SchemaWithDetails, error) {
    // This distinct on logic gets the latest version for each schema name
	query := `
		SELECT DISTINCT ON (s.name)
			s.id, s.project_name, s.name, s.version, 
			a.definition, p.definition
		FROM schemas s
		JOIN avro_schemas a ON s.id = a.schema_id
		JOIN parquet_schemas p ON s.id = p.schema_id
		WHERE s.project_name = $1
		ORDER BY s.name, s.version DESC`

	rows, err := db.Query(query, projectName)
	if err != nil {
		return nil, fmt.Errorf("error querying schemas: %v", err)
	}
	defer rows.Close()

	var schemas []models.SchemaWithDetails
	for rows.Next() {
		var s models.SchemaWithDetails
		if err := rows.Scan(&s.ID, &s.ProjectName, &s.Name, &s.Version, &s.AvroSchema, &s.ParquetSchema); err != nil {
			return nil, fmt.Errorf("error scanning schema: %v", err)
		}
		schemas = append(schemas, s)
	}

	return schemas, nil
}

// ... [RegisterProject, GetProjects, etc. remain unchanged] ...
func RegisterProject(db *sql.DB, project *models.ProjectMetadata) error {
    query := `INSERT INTO project (name, description, owner_id) VALUES ($1, $2, $3)`
    _, err := db.Exec(query, project.ProjectName, project.Description, project.Owner)
    if err != nil {
        return fmt.Errorf("error inserting project: %v", err)
    }
    return nil
}

func GetProjects(db *sql.DB, owner_id string) (map[string]uuid.UUID, error) {
    query := `SELECT name, owner_id FROM project WHERE owner_id = $1`
    rows, err := db.Query(query, owner_id)
    if err != nil {
        return nil, fmt.Errorf("error querying projects: %v", err)
    }
    defer rows.Close()

    projects := make(map[string]uuid.UUID)
    for rows.Next() {
        var name, idStr string
        if err := rows.Scan(&name, &idStr); err != nil {
            return nil, fmt.Errorf("error scanning project: %v", err)
        }
        id, err := uuid.Parse(idStr)
        if err != nil {
            return nil, fmt.Errorf("invalid UUID: %v", err)
        }
        projects[name] = id
    }
    return projects, nil
}

func CheckProjectExistence(db *sql.DB, projectName string) (bool, error) {
    var exists bool
    err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM project WHERE name = $1)", projectName).Scan(&exists)
    return exists, err
}

func GetProjectOwnerID(db *sql.DB, projectName string) (string, error) {
    var ownerID string
    err := db.QueryRow("SELECT owner_id FROM project WHERE name = $1", projectName).Scan(&ownerID)
    return ownerID, err
}