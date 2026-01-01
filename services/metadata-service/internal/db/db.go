package db

import (
	"database/sql"
	"encoding/json"
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

func GetDBConfig() (string, int, string, string, string) {
	host := getEnv("DB_HOST", "localhost") // Use Kubernetes service name
	portStr := getEnv("DB_PORT", "5432")
	port := 5432 // Default port
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

// TODO: Refactor this
func Connect_to_db(logger *logging.Logger) (*sql.DB, error) {
	host, port, user, password, dbname := GetDBConfig()

	defaultConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		host, port, user, password)

	logger.Info("Connecting to default database at %s:%d", zap.String("host", host), zap.Int("port", port))
	defaultDB, err := sql.Open("postgres", defaultConnStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to default database: %v", err)
	}
	defer defaultDB.Close()

	// Test the connection
	err = defaultDB.Ping()
	if err != nil {
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

	log.Printf("Connecting to target database '%s'", dbname)
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to target database: %v", err)
	}

	// Test the connection
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("could not ping target database: %v", err)
	}

	// Update path to use a relative path or environment variable
	sqlFilePath := getEnv("SQL_PATH", "./sql/create_tables.sql")
	sqlBytes, err := ioutil.ReadFile(sqlFilePath)
	if err != nil {
		return nil, fmt.Errorf("error reading SQL file from '%s': %v", sqlFilePath, err)
	}

	_, err = db.Exec(string(sqlBytes))
	if err != nil {
		return nil, fmt.Errorf("error executing SQL file: %v", err)
	}

	log.Println("Database tables created successfully")
	return db, nil
}

func CreateSchema(db *sql.DB, schema models.Schema) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	fieldsJSON, err := json.Marshal(schema.Fields)
	if err != nil {
		return fmt.Errorf("error marshalling fields to JSON: %v", err)
	}

	_, err = tx.Exec(
		"INSERT INTO schemas (project_name, name, version, fields) VALUES ($1, $2, $3, $4)",
		schema.ProjectName,
		schema.Name,
		schema.Version,
		fieldsJSON,
	)
	if err != nil {
		return fmt.Errorf("error inserting schema: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

func GetSchema(db *sql.DB, projectName, schemaName string) (*models.Schema, error) {
	var schema models.Schema
	var fieldsJSON string

	err := db.QueryRow(
		"SELECT project_name, name, version, fields FROM schemas WHERE project_name = $1 AND name = $2",
		projectName,
		schemaName,
	).Scan(&schema.ProjectName, &schema.Name, &schema.Version, &fieldsJSON)
	if err != nil {
		return nil, fmt.Errorf("error querying schema: %v", err)
	}

	err = json.Unmarshal([]byte(fieldsJSON), &schema.Fields)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling fields from JSON: %v", err)
	}

	return &schema, nil
}

func UpdateSchema(db *sql.DB, schema models.Schema) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	fieldsJSON, err := json.Marshal(schema.Fields)
	if err != nil {
		return fmt.Errorf("error marshalling fields to JSON: %v", err)
	}

	var oldSchema models.Schema
	var oldFieldsJSON []byte

	err = tx.QueryRow(
		"SELECT project_name, name, version, fields FROM schemas WHERE project_name = $1 AND name = $2",
		schema.ProjectName,
		schema.Name,
	).Scan(&oldSchema.ProjectName, &oldSchema.Name, &oldSchema.Version, &oldFieldsJSON)
	if err != nil {
		return fmt.Errorf("error querying schema: %v", err)
	}

	// Insert the old schema into history_schemas
	_, err = tx.Exec(
		"INSERT INTO history_schemas (project_name, name, version, fields) VALUES ($1, $2, $3, $4)",
		oldSchema.ProjectName,
		oldSchema.Name,
		oldSchema.Version,
		oldFieldsJSON, // Use the raw JSON data directly
	)
	if err != nil {
		return fmt.Errorf("error inserting schema into history: %v", err)
	}

	// Update the schema with a new version
	_, err = tx.Exec(
		"UPDATE schemas SET version = $1, fields = $2 WHERE project_name = $3 AND name = $4",
		oldSchema.Version+1,
		fieldsJSON,
		schema.ProjectName,
		schema.Name,
	)
	if err != nil {
		return fmt.Errorf("error updating schema: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

func RegisterProject(db *sql.DB, project *models.ProjectMetadata) error {
	query := `INSERT INTO project (name, description, owner_id) VALUES ($1, $2, $3)`
	_, err := db.Exec(query, project.ProjectName, project.Description, project.Owner)
	if err != nil {
		return fmt.Errorf("error inserting project: %v", err)
	}
	return nil
}

func GetProjects(db *sql.DB, username string) (map[string]uuid.UUID, error) {
	query := `SELECT p.name, p.owner_id FROM project p
			  JOIN users u ON p.owner_id = u.id
			  WHERE u.username = $1`

	rows, err := db.Query(query, username)
	if err != nil {
		return nil, fmt.Errorf("error querying projects: %v", err)
	}
	defer rows.Close()

	projects := make(map[string]uuid.UUID)
	for rows.Next() {
		var name string
		var idStr string
		if err := rows.Scan(&name, &idStr); err != nil {
			return nil, fmt.Errorf("error scanning project: %v", err)
		}

		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("invalid UUID format for project owner_id: %v", err)
		}

		projects[name] = id
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over projects: %v", err)
	}

	return projects, nil
}

func CheckProjectExistence(db *sql.DB, projectName string) (bool, error) {
	query := `SELECT EXISTS(SELECT 1 FROM project WHERE name = $1)`
	var exists bool
	err := db.QueryRow(query, projectName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking project existence: %v", err)
	}
	return exists, nil
}
