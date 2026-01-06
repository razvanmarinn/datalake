package models


type SchemaWithDetails struct {
    ID            int             `json:"id"`
    ProjectName   string          `json:"project_name"`
    Name          string          `json:"name"`
    Version       int             `json:"version"`
    AvroSchema    string          `json:"avro_schema"`    
    ParquetSchema string          `json:"parquet_schema"`
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}


type ProjectMetadata struct {
	ProjectName string `json:"project_name"`
	Description string `json:"description"`
	Owner       string `json:"owner"`
	CreatedAt   string `json:"created_at"`
	Schemas    []SchemaWithDetails `json:"schemas"`
	SchemaCount int     `json:"schema_count"`
	KafkaTopic string `json:"kafka_topic"`
	
}