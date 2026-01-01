package models

type Schema struct {
	ID          int     `json:"id"`
	ProjectName string  `json:"project"`
	Name        string  `json:"name"`
	Fields      []Field `json:"fields"`
	Version     int     `json:"version"`
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
	Schemas    []Schema `json:"schemas"`
	SchemaCount int     `json:"schema_count"`
	KafkaTopic string `json:"kafka_topic"`
	
}