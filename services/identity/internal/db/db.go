package db

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/google/uuid"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/identity-service/internal/db/models"
	"golang.org/x/crypto/bcrypt"

	_ "github.com/lib/pq"
)

func GetDBConfig() (string, int, string, string, string) {
	host := getEnv("DB_HOST", "identity-postgres")
	portStr := getEnv("DB_PORT", "5432")
	port := 5432
	if portStr != "" {
		fmt.Sscanf(portStr, "%d", &port)
	}
	user := getEnv("DB_USER", "identityuser")
	password := getEnv("DB_PASSWORD", "identitypassword")
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

func Connect_to_db(logger *logging.Logger) (*sql.DB, error) {
	host, port, user, password, dbname := GetDBConfig()

	defaultConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		host, port, user, password)

	logger.LogDatabaseConnection(host, port, "postgres")
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
		logger.WithDatabase("create").Info("Database created successfully")
	}

	dbConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	logger.LogDatabaseConnection(host, port, dbname)
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

	logger.LogDatabaseSuccess(dbname)
	return db, nil
}

func RegisterUser(db *sql.DB, user *models.Client) error {
	query := `INSERT INTO users (username, email, password) VALUES ($1, $2, $3)`

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	_, err = db.Exec(query, user.Username, user.Email, string(hashedPassword))
	if err != nil {
		return fmt.Errorf("error inserting user: %v", err)
	}
	return nil
}

func GetUser(db *sql.DB, username string) (*models.Client, error) {
	query := `SELECT id, username, email, password FROM users WHERE username = $1`
	row := db.QueryRow(query, username)

	var user models.Client
	err := row.Scan(&user.ID, &user.Username, &user.Email, &user.Password)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // User not found
		}
		return nil, fmt.Errorf("error scanning user: %v", err)
	}
	return &user, nil
}

func GetUserByID(db *sql.DB, id uuid.UUID) (*models.Client, error) {
	var user models.Client
	query := `SELECT id, username, password FROM clients WHERE id=$1`
	err := db.QueryRow(query, id).Scan(&user.ID, &user.Username, &user.Password)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &user, nil
}
