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
