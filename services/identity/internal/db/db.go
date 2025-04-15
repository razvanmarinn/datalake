package db

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/razvanmarinn/identity_service/internal/db/models"

	_ "github.com/lib/pq"
)

func GetDBConfig() (string, int, string, string, string) {
	host := getEnv("DB_HOST", "localhost")
	portStr := getEnv("DB_PORT", "5432")
	port := 5432
	if portStr != "" {
		fmt.Sscanf(portStr, "%d", &port)
	}
	user := getEnv("DB_USER", "postgres")
	password := getEnv("DB_PASSWORD", "1234")
	dbname := getEnv("DB_NAME", "identity_db")

	return host, port, user, password, dbname
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func Connect_to_db() (*sql.DB, error) {
	host, port, user, password, dbname := GetDBConfig()

	defaultConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		host, port, user, password)

	log.Printf("Connecting to default database at %s:%d", host, port)
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
	sqlFilePath := getEnv("SQL_FILE_PATH", "sql/create_tables.sql")
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

func RegisterUser(db *sql.DB, user *models.Client) error {
	query := `INSERT INTO users (username, email, password) VALUES ($1, $2, $3)`
	_, err := db.Exec(query, user.Username, user.Email, user.Password)
	if err != nil {
		return fmt.Errorf("error inserting user: %v", err)
	}
	return nil
}

func RegisterProject(db *sql.DB, project *models.Project) error {
	query := `INSERT INTO project (name, description, owner_id) VALUES ($1, $2, $3)`
	_, err := db.Exec(query, project.Name, project.Description, project.OwnerID)
	if err != nil {
		return fmt.Errorf("error inserting project: %v", err)
	}
	return nil
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
