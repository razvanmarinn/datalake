package db

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"go.uber.org/zap"

	"github.com/razvanmarinn/metadata-service/internal/models"

	_ "github.com/lib/pq"
)

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

// --- Project Management ---

func RegisterProject(db *sql.DB, project *models.ProjectMetadata) error {
	// Assuming created_at defaults to NOW() in DB schema
	query := `INSERT INTO project (name, description, owner_id) VALUES ($1, $2, $3)`
	_, err := db.Exec(query, project.ProjectName, project.Description, project.Owner)
	if err != nil {
		return fmt.Errorf("error inserting project: %v", err)
	}
	return nil
}

func GetProjectByID(db *sql.DB, projectID uuid.UUID) (*models.ProjectMetadata, error) {
	query := `SELECT id, name, owner_id, created_at FROM project WHERE id = $1`

	var p models.ProjectMetadata
	// You might need to update models.ProjectMetadata to include ID and CreatedAt if not present
	var idStr string

	err := db.QueryRow(query, projectID).Scan(&idStr, &p.ProjectName, &p.Owner, &p.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func GetProjectUUIDByProjectName(db *sql.DB, projectName string) (uuid.UUID, error) {
	var projectID uuid.UUID
	err := db.QueryRow("SELECT id FROM project WHERE name = $1", projectName).Scan(&projectID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to get project ID for %s: %w", projectName, err)
	}
	return projectID, nil
}

func GetProjects(db *sql.DB, owner_id string) (map[string]uuid.UUID, error) {
	query := `SELECT id, name FROM project WHERE owner_id = $1`

	rows, err := db.Query(query, owner_id)
	if err != nil {
		return nil, fmt.Errorf("error querying projects: %v", err)
	}
	defer rows.Close()

	projects := make(map[string]uuid.UUID)
	for rows.Next() {
		var idStr, name string

		if err := rows.Scan(&idStr, &name); err != nil {
			return nil, fmt.Errorf("error scanning project: %v", err)
		}

		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("invalid UUID for project %s: %v", name, err)
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

func GetProjectID(db *sql.DB, projectName string) (uuid.UUID, error) {
	var projectID uuid.UUID
	err := db.QueryRow("SELECT id FROM project WHERE name = $1", projectName).Scan(&projectID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to get project ID for %s: %w", projectName, err)
	}
	return projectID, nil
}

func GetProjectNameByID(db *sql.DB, projectID uuid.UUID) (string, error) {
	var name string
	err := db.QueryRow("SELECT name FROM project WHERE id = $1", projectID).Scan(&name)
	if err != nil {
		return "", fmt.Errorf("failed to find project name for id %s: %w", projectID, err)
	}
	return name, nil
}

// --- Schema Management ---

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

func GetSchemaByID(db *sql.DB, schemaID int) (*models.SchemaWithDetails, error) {
	query := `
        SELECT
            s.id, s.project_name, s.name, s.version,
            a.definition as avro_def,
            p.definition as parquet_def
        FROM schemas s
        JOIN avro_schemas a ON s.id = a.schema_id
        JOIN parquet_schemas p ON s.id = p.schema_id
        WHERE s.id = $1`

	var s models.SchemaWithDetails
	err := db.QueryRow(query, schemaID).Scan(
		&s.ID,
		&s.ProjectName,
		&s.Name,
		&s.Version,
		&s.AvroSchema,
		&s.ParquetSchema,
	)

	if err != nil {
		return nil, err
	}

	return &s, nil
}

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

func GetSchemaID(db *sql.DB, projectName, schemaName string) (int, error) {
	var schemaID int
	err := db.QueryRow("SELECT id FROM schemas WHERE project_name = $1 AND name = $2 ORDER BY version DESC LIMIT 1", projectName, schemaName).Scan(&schemaID)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema ID for project %s, schema %s: %w", projectName, schemaName, err)
	}
	return schemaID, nil
}

// --- Data & Job Management ---
func RegisterDataFile(db *sql.DB, projectIDStr, schemaName, blockID, workerID, filePath string, fileSize int64, fileFormat string) error {
	projectUUID, err := uuid.Parse(projectIDStr)
	if err != nil {
		return fmt.Errorf("invalid project uuid '%s': %v", projectIDStr, err)
	}

	projectName, err := GetProjectNameByID(db, projectUUID)
	if err != nil {
		return fmt.Errorf("failed to resolve project name for id %s: %v", projectIDStr, err)
	}

	schemaID, err := GetSchemaID(db, projectName, schemaName)
	if err != nil {
		return fmt.Errorf("failed to resolve schema id for '%s' in project '%s': %v", schemaName, projectName, err)
	}

	// 3. Call the existing low-level RegisterBlock
	block := models.Block{
		BlockID:  blockID,
		WorkerID: workerID,
		Path:     filePath,
		Size:     fileSize,
		Format:   fileFormat,
	}

	if err := RegisterBlock(db, projectUUID, schemaID, block); err != nil {
		return fmt.Errorf("failed to register block in db: %v", err)
	}

	return nil
}

// RegisterBlock inserts a new data file record
func RegisterBlock(db *sql.DB, projectID uuid.UUID, schemaID int, block models.Block) error {
	query := `INSERT INTO data_files (project_id, schema_id, block_id, worker_id, path, size, format) VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err := db.Exec(query, projectID, schemaID, block.BlockID, block.WorkerID, block.Path, block.Size, block.Format)
	if err != nil {
		return fmt.Errorf("error inserting block record: %v", err)
	}
	return nil
}

// GetBlockLocations retrieves block locations for a list of block IDs
func GetBlockLocations(db *sql.DB, blockIDs []string) ([]models.BlockLocation, error) {
	query := `SELECT block_id, worker_id, path FROM data_files WHERE block_id = ANY($1)`
	rows, err := db.Query(query, pq.Array(blockIDs))
	if err != nil {
		return nil, fmt.Errorf("error querying block locations: %v", err)
	}
	defer rows.Close()

	var locations []models.BlockLocation
	for rows.Next() {
		var loc models.BlockLocation
		if err := rows.Scan(&loc.BlockID, &loc.WorkerID, &loc.Path); err != nil {
			return nil, fmt.Errorf("error scanning block location: %v", err)
		}
		locations = append(locations, loc)
	}
	return locations, nil
}

func CreateCompactionJob(db *sql.DB, projectID uuid.UUID, schemaID int, targetBlockIDs []string) (*models.CompactionJob, error) {
	job := &models.CompactionJob{
		ID:             uuid.New(),
		ProjectID:      projectID,
		SchemaID:       schemaID,
		Status:         "PENDING",
		TargetBlockIDs: targetBlockIDs,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	query := `INSERT INTO compaction_jobs (id, project_id, schema_id, status, target_block_ids, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err := db.Exec(query, job.ID, job.ProjectID, job.SchemaID, job.Status, pq.Array(job.TargetBlockIDs), job.CreatedAt, job.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("error creating compaction job: %v", err)
	}
	return job, nil
}
func PollPendingCompactionJobs(db *sql.DB) ([]*models.CompactionJob, error) {
	query := `
		SELECT
			j.id,
			j.project_id,
			p.name,
			j.schema_id,
			s.name,
			j.status,
			j.target_block_ids,
			j.created_at,
			j.updated_at
		FROM compaction_jobs j
		JOIN schemas s ON j.schema_id = s.id
		JOIN project p ON j.project_id = p.id
		WHERE j.status = 'PENDING'
		ORDER BY j.created_at ASC
		FOR UPDATE SKIP LOCKED
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to poll jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.CompactionJob

	for rows.Next() {
		job := &models.CompactionJob{}
		var targetBlockIDs []sql.NullString

		err := rows.Scan(
			&job.ID,
			&job.ProjectID,
			&job.ProjectName,
			&job.SchemaID,
			&job.SchemaName,
			&job.Status,
			pq.Array(&targetBlockIDs),
			&job.CreatedAt,
			&job.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job: %w", err)
		}

		job.TargetBlockIDs = make([]string, 0, len(targetBlockIDs))
		for _, s := range targetBlockIDs {
			if s.Valid {
				job.TargetBlockIDs = append(job.TargetBlockIDs, s.String)
			}
		}

		jobs = append(jobs, job)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating jobs: %w", err)
	}

	pathQuery := `SELECT path FROM data_files WHERE block_id = ANY($1)`

	for _, job := range jobs {
		if len(job.TargetBlockIDs) == 0 {
			continue
		}

		pRows, err := db.Query(pathQuery, pq.Array(job.TargetBlockIDs))
		if err != nil {
			return nil, fmt.Errorf("failed to resolve paths for job %s: %w", job.ID, err)
		}

		func() {
			defer pRows.Close()
			var paths []string
			for pRows.Next() {
				var p string
				if err := pRows.Scan(&p); err == nil {
					paths = append(paths, p)
				}
			}
			job.TargetPaths = paths
		}()
	}

	return jobs, nil
}
func UpdateCompactionJobStatus(db *sql.DB, jobID uuid.UUID, status, outputFilePath string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	queryUpdateJob := `
		UPDATE compaction_jobs
		SET status = $1, output_file_path = $2, updated_at = $3
		WHERE id = $4
		RETURNING target_block_ids`

	var targetBlockIDs []string
	err = tx.QueryRow(queryUpdateJob, status, outputFilePath, time.Now(), jobID).Scan(pq.Array(&targetBlockIDs))
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	if status == "COMPLETED" && len(targetBlockIDs) > 0 {
		queryMarkCompacted := `
			UPDATE data_files
			SET is_compacted = TRUE
			WHERE block_id = ANY($1)`

		res, err := tx.Exec(queryMarkCompacted, pq.Array(targetBlockIDs))
		if err != nil {
			return fmt.Errorf("failed to mark files as compacted: %w", err)
		}

		rowsAffected, _ := res.RowsAffected()
		log.Printf("Marked %d files as compacted for Job %s", rowsAffected, jobID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func GetUncompactedFileStats(db *sql.DB) ([]models.UncompactedFileStat, error) {
	query := `
        SELECT project_id, schema_id, COUNT(*) as file_count
        FROM data_files
        WHERE is_compacted = FALSE
        GROUP BY project_id, schema_id`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying uncompacted file stats: %v", err)
	}
	defer rows.Close()

	var stats []models.UncompactedFileStat
	for rows.Next() {
		var stat models.UncompactedFileStat
		if err := rows.Scan(&stat.ProjectID, &stat.SchemaID, &stat.FileCount); err != nil {
			return nil, fmt.Errorf("error scanning uncompacted file stat: %v", err)
		}
		stats = append(stats, stat)
	}
	return stats, nil
}

// GetUncompactedBlockIDs retrieves a list of uncompacted block IDs for a given project and schema
func GetUncompactedBlockIDs(db *sql.DB, projectID uuid.UUID, schemaID, limit int) ([]string, error) {
	query := `
        SELECT block_id
        FROM data_files
        WHERE project_id = $1 AND schema_id = $2 AND is_compacted = FALSE
        LIMIT $3`

	rows, err := db.Query(query, projectID, schemaID, limit)
	if err != nil {
		return nil, fmt.Errorf("error querying uncompacted block IDs: %v", err)
	}
	defer rows.Close()

	var blockIDs []string
	for rows.Next() {
		var blockID string
		if err := rows.Scan(&blockID); err != nil {
			return nil, fmt.Errorf("error scanning uncompacted block ID: %v", err)
		}
		blockIDs = append(blockIDs, blockID)
	}
	return blockIDs, nil
}

func GetBlockIDsInPendingCompactionJobs(db *sql.DB, projectID uuid.UUID, schemaID int) (map[string]bool, error) {
	// FIX: Added 'RUNNING' to status check to catch actively processing jobs
	query := `
		SELECT unnest(target_block_ids)
		FROM compaction_jobs
		WHERE status IN ('PENDING', 'RUNNING')
          AND project_id = $1
          AND schema_id = $2`

	rows, err := db.Query(query, projectID, schemaID)
	if err != nil {
		return nil, fmt.Errorf("error querying pending compaction block IDs: %w", err)
	}
	defer rows.Close()

	lockedBlocks := make(map[string]bool)
	for rows.Next() {
		var blockID string
		if err := rows.Scan(&blockID); err != nil {
			return nil, fmt.Errorf("error scanning block ID: %w", err)
		}
		lockedBlocks[blockID] = true
	}

	return lockedBlocks, nil
}
