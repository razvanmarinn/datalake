package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	cl "github.com/razvanmarinn/rcss/internal/client"
	batchingprocessor "github.com/razvanmarinn/rcss/pkg/batching_processor"
)

var rcssClient *cl.RCSSClient

func main() {
	// Initialize router and client
	router := mux.NewRouter()
	rcssClient = cl.NewRCSSClient()

	// Define API endpoints
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Hello, World!")
	})
	router.HandleFunc("/file/{fileName}", getFileFromMaster).Methods("GET")
	router.HandleFunc("/file/{fileName}", setFileToMaster).Methods("POST")

	log.Println("API is running on port 8000...")
	http.ListenAndServe(":8000", router)
}

func getFileFromMaster(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileName := vars["fileName"]

	if fileName == "" {
		http.Error(w, "File name is required", http.StatusBadRequest)
		return
	}

	fileContent, err := rcssClient.GetFileBackFromWorkers(fileName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error retrieving file: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", fileName))

	_, err = w.Write(fileContent)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error writing response: %v", err), http.StatusInternalServerError)
		return
	}
}

func setFileToMaster(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(1 << 30)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing form: %v", err), http.StatusBadRequest)
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		http.Error(w, fmt.Sprintf("Error retrieving file: %v", err), http.StatusBadRequest)
		return
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading file: %v", err), http.StatusInternalServerError)
		return
	}

	fileName := handler.Filename
	batches := batchingprocessor.NewBatchProcessor(fileBytes).Process()
	err = rcssClient.RegisterFileMetadata(fileName, batches)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error processing file: %v", err), http.StatusInternalServerError)
		return
	}



	for _, batch := range batches {
		wIp, wPort, err := rcssClient.GetBatchDest(batch.UUID, int32(len(batch.Data)))
		if err != nil {
			fmt.Errorf("Error %s", err)
		}
		rcssClient.SendBatchToWorkers(wIp, wPort, batch.UUID, batch.Data)
	}
	

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "File %s processed successfully", fileName)
}
