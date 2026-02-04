package db

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/razvanmarinn/identity-service/internal/db/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterUser_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	user := &models.Client{
		Username: "testuser",
		Email:    "test@example.com",
		Password: "password123",
	}

	mock.ExpectExec("INSERT INTO users").
		WithArgs(user.Username, user.Email, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = RegisterUser(db, user)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRegisterUser_DuplicateUser(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	user := &models.Client{
		Username: "existinguser",
		Email:    "existing@example.com",
		Password: "password123",
	}

	mock.ExpectExec("INSERT INTO users").
		WithArgs(user.Username, user.Email, sqlmock.AnyArg()).
		WillReturnError(assert.AnError)

	err = RegisterUser(db, user)
	assert.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetUser_Found(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	userID := uuid.New()
	expectedUser := &models.Client{
		ID:       userID,
		Username: "testuser",
		Email:    "test@example.com",
		Password: "hashedpassword",
	}

	rows := sqlmock.NewRows([]string{"id", "username", "email", "password"}).
		AddRow(userID, "testuser", "test@example.com", "hashedpassword")

	mock.ExpectQuery("SELECT id, username, email, password FROM users WHERE username").
		WithArgs("testuser").
		WillReturnRows(rows)

	user, err := GetUser(db, "testuser")
	assert.NoError(t, err)
	assert.NotNil(t, user)
	assert.Equal(t, expectedUser.Username, user.Username)
	assert.Equal(t, expectedUser.Email, user.Email)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetUser_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "username", "email", "password"})

	mock.ExpectQuery("SELECT id, username, email, password FROM users WHERE username").
		WithArgs("nonexistent").
		WillReturnRows(rows)

	user, err := GetUser(db, "nonexistent")
	assert.NoError(t, err)
	assert.Nil(t, user)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetUser_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT id, username, email, password FROM users WHERE username").
		WithArgs("testuser").
		WillReturnError(assert.AnError)

	user, err := GetUser(db, "testuser")
	assert.Error(t, err)
	assert.Nil(t, user)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetUserByID_Found(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	userID := uuid.New()

	rows := sqlmock.NewRows([]string{"id", "username", "password"}).
		AddRow(userID, "testuser", "hashedpassword")

	mock.ExpectQuery("SELECT id, username, password FROM clients WHERE id").
		WithArgs(userID).
		WillReturnRows(rows)

	user, err := GetUserByID(db, userID)
	assert.NoError(t, err)
	assert.NotNil(t, user)
	assert.Equal(t, userID, user.ID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetUserByID_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	userID := uuid.New()
	rows := sqlmock.NewRows([]string{"id", "username", "password"})

	mock.ExpectQuery("SELECT id, username, password FROM clients WHERE id").
		WithArgs(userID).
		WillReturnRows(rows)

	user, err := GetUserByID(db, userID)
	assert.NoError(t, err)
	assert.Nil(t, user)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetDBConfig_Defaults(t *testing.T) {
	host, port, user, password, dbname := GetDBConfig()
	assert.NotEmpty(t, host)
	assert.Equal(t, 5432, port)
	assert.NotEmpty(t, user)
	assert.NotEmpty(t, password)
	assert.NotEmpty(t, dbname)
}

func TestGetDBConfig_CustomEnv(t *testing.T) {
	t.Setenv("DB_HOST", "custom-host")
	t.Setenv("DB_PORT", "5433")
	t.Setenv("DB_USER", "customuser")
	t.Setenv("DB_PASSWORD", "custompass")
	t.Setenv("DB_NAME", "customdb")

	host, port, user, password, dbname := GetDBConfig()
	assert.Equal(t, "custom-host", host)
	assert.Equal(t, 5433, port)
	assert.Equal(t, "customuser", user)
	assert.Equal(t, "custompass", password)
	assert.Equal(t, "customdb", dbname)
}

func TestGetEnv_Default(t *testing.T) {
	result := getEnv("NONEXISTENT_VAR_XYZ123", "defaultval")
	assert.Equal(t, "defaultval", result)
}

func TestGetEnv_EnvSet(t *testing.T) {
	t.Setenv("TEST_ENV_VAR", "actualvalue")
	result := getEnv("TEST_ENV_VAR", "defaultval")
	assert.Equal(t, "actualvalue", result)
}

func TestRegisterUser_EmptyPassword(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	user := &models.Client{
		Username: "testuser",
		Email:    "test@example.com",
		Password: "",
	}

	mock.ExpectExec("INSERT INTO users").
		WithArgs(user.Username, user.Email, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = RegisterUser(db, user)
	assert.NoError(t, err)
}

func TestGetUser_MultipleUsers(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	userID := uuid.New()
	rows := sqlmock.NewRows([]string{"id", "username", "email", "password"}).
		AddRow(userID, "user1", "user1@test.com", "hash1")

	mock.ExpectQuery("SELECT id, username, email, password FROM users WHERE username").
		WithArgs("user1").
		WillReturnRows(rows)

	user, err := GetUser(db, "user1")
	assert.NoError(t, err)
	assert.NotNil(t, user)
	assert.Equal(t, "user1", user.Username)
}

func TestGetUserByID_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	userID := uuid.New()

	mock.ExpectQuery("SELECT id, username, password FROM clients WHERE id").
		WithArgs(userID).
		WillReturnError(assert.AnError)

	user, err := GetUserByID(db, userID)
	assert.Error(t, err)
	assert.Nil(t, user)
}

func TestGetDBConfig_InvalidPort(t *testing.T) {
	t.Setenv("DB_PORT", "invalid")
	host, port, _, _, _ := GetDBConfig()
	assert.NotEmpty(t, host)
	assert.Equal(t, 5432, port) // Should use default
}

func TestGetDBConfig_EmptyEnv(t *testing.T) {
	t.Setenv("DB_HOST", "")
	host, _, _, _, _ := GetDBConfig()
	assert.Equal(t, "identity-postgres", host) // Default value
}
