package main

import (
	"html/template"
	"log"
	"net/http"
)

type Dataset struct {
	Name string
	Path string
}

var files = []Dataset{
	{Name: "users.parquet", Path: "/data/users.parquet"},
	{Name: "events.avro", Path: "/data/events.avro"},
	{Name: "sales.csv", Path: "/data/sales.csv"},
}

func main() {
	tmpl := template.Must(template.ParseGlob("../templates/*.html"))

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		err := tmpl.ExecuteTemplate(w, "index.html", struct{ Files []Dataset }{files})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	http.HandleFunc("/file", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		schema := mockSchema(id)

		err := tmpl.ExecuteTemplate(w, "file.html", struct {
			FileName string
			FilePath string
			Schema   []Field
		}{
			FileName: id,
			FilePath: id,
			Schema:   schema,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	log.Println("Listening on http://localhost:8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}

type Field struct {
	Name string
	Type string
}

func mockSchema(id string) []Field {
	return []Field{
		{"id", "int"},
		{"name", "string"},
		{"timestamp", "datetime"},
	}
}
