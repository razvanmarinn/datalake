package db

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/razvanmarinn/metadata-service/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterProject_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	project := &models.ProjectMetadata{
		ProjectName: "test-project",
		Description: "Test description",
		Owner:       "owner-123",
	}

	mock.ExpectExec("INSERT INTO project").
		WithArgs(project.ProjectName, project.Description, project.Owner).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = RegisterProject(db, project)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRegisterProject_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	project := &models.ProjectMetadata{
		ProjectName: "test-project",
		Owner:       "owner-123",
	}

	mock.ExpectExec("INSERT INTO project").
		WillReturnError(assert.AnError)

	err = RegisterProject(db, project)
	assert.Error(t, err)
}

func TestGetProjectByID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	projectID := uuid.New()
	rows := sqlmock.NewRows([]string{"id", "name", "owner_id", "created_at"}).
		AddRow(projectID.String(), "test-project", "owner-123", time.Now())

	mock.ExpectQuery("SELECT id, name, owner_id, created_at FROM project WHERE id").
		WithArgs(projectID).
		WillReturnRows(rows)

	project, err := GetProjectByID(db, projectID)
	assert.NoError(t, err)
	assert.NotNil(t, project)
	assert.Equal(t, "test-project", project.ProjectName)
}

func TestGetProjectUUIDByProjectName_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	projectID := uuid.New()
	rows := sqlmock.NewRows([]string{"id"}).AddRow(projectID)

	mock.ExpectQuery("SELECT id FROM project WHERE name").
		WithArgs("test-project").
		WillReturnRows(rows)

	result, err := GetProjectUUIDByProjectName(db, "test-project")
	assert.NoError(t, err)
	assert.Equal(t, projectID, result)
}

func TestGetProjects_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	id1 := uuid.New()
	id2 := uuid.New()
	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(id1.String(), "project1").
		AddRow(id2.String(), "project2")

	mock.ExpectQuery("SELECT id, name FROM project WHERE owner_id").
		WithArgs("owner-123").
		WillReturnRows(rows)

	projects, err := GetProjects(db, "owner-123")
	assert.NoError(t, err)
	assert.Len(t, projects, 2)
}

func TestCheckProjectExistence_Exists(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)

	mock.ExpectQuery("SELECT EXISTS").
		WithArgs("test-project").
		WillReturnRows(rows)

	exists, err := CheckProjectExistence(db, "test-project")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestCheckProjectExistence_NotExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"exists"}).AddRow(false)

	mock.ExpectQuery("SELECT EXISTS").
		WithArgs("nonexistent").
		WillReturnRows(rows)

	exists, err := CheckProjectExistence(db, "nonexistent")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestGetProjectOwnerID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"owner_id"}).AddRow("owner-123")

	mock.ExpectQuery("SELECT owner_id FROM project WHERE name").
		WithArgs("test-project").
		WillReturnRows(rows)

	ownerID, err := GetProjectOwnerID(db, "test-project")
	assert.NoError(t, err)
	assert.Equal(t, "owner-123", ownerID)
}

func TestGetProjectID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	projectID := uuid.New()
	rows := sqlmock.NewRows([]string{"id"}).AddRow(projectID)

	mock.ExpectQuery("SELECT id FROM project WHERE name").
		WithArgs("test-project").
		WillReturnRows(rows)

	result, err := GetProjectID(db, "test-project")
	assert.NoError(t, err)
	assert.Equal(t, projectID, result)
}

func TestGetProjectNameByID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	projectID := uuid.New()
	rows := sqlmock.NewRows([]string{"name"}).AddRow("test-project")

	mock.ExpectQuery("SELECT name FROM project WHERE id").
		WithArgs(projectID).
		WillReturnRows(rows)

	name, err := GetProjectNameByID(db, projectID)
	assert.NoError(t, err)
	assert.Equal(t, "test-project", name)
}

func TestCreateSchema_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	schema := models.SchemaWithDetails{
		ProjectName:   "test-project",
		Name:          "test-schema",
		AvroSchema:    `{"type":"record"}`,
		ParquetSchema: "parquet-def",
		Version:       1,
	}

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO schemas").
		WithArgs(schema.ProjectName, schema.Name).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectExec("INSERT INTO avro_schemas").
		WithArgs(1, schema.AvroSchema).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO parquet_schemas").
		WithArgs(1, schema.ParquetSchema).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = CreateSchema(db, schema)
	assert.NoError(t, err)
}

func TestRegisterBlock_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	projectID := uuid.New()
	block := models.Block{
		BlockID:  "block-123",
		WorkerID: "worker-456",
		Path:     "/data/file.parquet",
		Size:     1024,
		Format:   "parquet",
	}

	mock.ExpectExec("INSERT INTO data_files").
		WithArgs(projectID, 1, block.BlockID, block.WorkerID, block.Path, block.Size, block.Format).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = RegisterBlock(db, projectID, 1, block)
	assert.NoError(t, err)
}

func TestGetBlockLocations_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	blockIDs := []string{"block-1", "block-2"}
	rows := sqlmock.NewRows([]string{"block_id", "worker_id", "path"}).
		AddRow("block-1", "worker-1", "/path/1").
		AddRow("block-2", "worker-2", "/path/2")

	mock.ExpectQuery("SELECT block_id, worker_id, path FROM data_files WHERE block_id").
		WithArgs(pq.Array(blockIDs)).
		WillReturnRows(rows)

	locations, err := GetBlockLocations(db, blockIDs)
	assert.NoError(t, err)
	assert.Len(t, locations, 2)
}

func TestCreateCompactionJob_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	projectID := uuid.New()
	blockIDs := []string{"block-1", "block-2"}

	mock.ExpectExec("INSERT INTO compaction_jobs").
		WillReturnResult(sqlmock.NewResult(1, 1))

	job, err := CreateCompactionJob(db, projectID, 1, blockIDs)
	assert.NoError(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, "PENDING", job.Status)
}

func TestGetUncompactedFileStats_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	projectID := uuid.New()
	rows := sqlmock.NewRows([]string{"project_id", "schema_id", "file_count"}).
		AddRow(projectID, 1, 10)

	mock.ExpectQuery("SELECT project_id, schema_id, COUNT").
		WillReturnRows(rows)

	stats, err := GetUncompactedFileStats(db)
	assert.NoError(t, err)
	assert.Len(t, stats, 1)
	assert.Equal(t, int64(10), stats[0].FileCount)
}

func TestGetUncompactedBlockIDs_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	projectID := uuid.New()
	rows := sqlmock.NewRows([]string{"block_id"}).
		AddRow("block-1").
		AddRow("block-2")

	mock.ExpectQuery("SELECT block_id FROM data_files WHERE project_id").
		WithArgs(projectID, 1, 10).
		WillReturnRows(rows)

	blockIDs, err := GetUncompactedBlockIDs(db, projectID, 1, 10)
	assert.NoError(t, err)
	assert.Len(t, blockIDs, 2)
}

func TestGetDBConfig_Defaults(t *testing.T) {
	host, port, user, password, dbname := GetDBConfig()
	assert.NotEmpty(t, host)
	assert.Equal(t, 5432, port)
	assert.NotEmpty(t, user)
	assert.NotEmpty(t, password)
	assert.NotEmpty(t, dbname)
}

func TestGetEnv_Default(t *testing.T) {
	result := getEnv("NONEXISTENT_VAR_XYZ", "defaultval")
	assert.Equal(t, "defaultval", result)
}

func TestGetEnv_Set(t *testing.T) {
	t.Setenv("TEST_VAR_123", "actualvalue")
	result := getEnv("TEST_VAR_123", "default")
	assert.Equal(t, "actualvalue", result)
}

func TestGetSchemaByID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "project_name", "name", "version", "avro_def", "parquet_def"}).
		AddRow(1, "test-project", "test-schema", 1, `{"type":"record"}`, "parquet-def")

	mock.ExpectQuery("SELECT").
		WithArgs(1).
		WillReturnRows(rows)

	schema, err := GetSchemaByID(db, 1)
	assert.NoError(t, err)
	assert.NotNil(t, schema)
	assert.Equal(t, "test-schema", schema.Name)
}

func TestGetSchema_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "project_name", "name", "version", "avro_def", "parquet_def"}).
		AddRow(1, "test-project", "test-schema", 1, `{"type":"record"}`, "parquet-def")

	mock.ExpectQuery("SELECT").
		WithArgs("test-project", "test-schema").
		WillReturnRows(rows)

	schema, err := GetSchema(db, "test-project", "test-schema")
	assert.NoError(t, err)
	assert.NotNil(t, schema)
}

func TestGetSchemaID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id"}).AddRow(1)

	mock.ExpectQuery("SELECT id FROM schemas WHERE project_name").
		WithArgs("test-project", "test-schema").
		WillReturnRows(rows)

	id, err := GetSchemaID(db, "test-project", "test-schema")
	assert.NoError(t, err)
	assert.Equal(t, 1, id)
}

func TestListSchemas_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "project_name", "name", "version", "definition", "definition"}).
		AddRow(1, "test-project", "schema1", 1, `{}`, "parquet1").
		AddRow(2, "test-project", "schema2", 1, `{}`, "parquet2")

	mock.ExpectQuery("SELECT DISTINCT ON").
		WithArgs("test-project").
		WillReturnRows(rows)

	schemas, err := ListSchemas(db, "test-project")
	assert.NoError(t, err)
	assert.Len(t, schemas, 2)
}

func TestGetBlockIDsInPendingCompactionJobs_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	projectID := uuid.New()
	rows := sqlmock.NewRows([]string{"unnest"}).
		AddRow("block-1").
		AddRow("block-2")

	mock.ExpectQuery("SELECT unnest").
		WithArgs(projectID, 1).
		WillReturnRows(rows)

	blocks, err := GetBlockIDsInPendingCompactionJobs(db, projectID, 1)
	assert.NoError(t, err)
	assert.Len(t, blocks, 2)
	assert.True(t, blocks["block-1"])
}
