package nodes

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"

	pb "github.com/razvanmarinn/datalake/protobuf"
)

func (wn *WorkerNode) Start() {
	fmt.Println("WorkerNode started")
}

func (wn *WorkerNode) Stop() {
}

func (wn *WorkerNode) HealthCheck() bool {
	return true
}

func (w *WorkerNode) ReceiveBatch(ctx context.Context, req *pb.SendClientRequestToWorker) (*pb.WorkerResponse, error) {
	batchID := req.BatchId
	w.ReceivedBatches[batchID] = req.Data
	w.SaveBatch(batchID, FileFormat(req.FileType))
	log.Printf("Received and stored batch ID: %s, Data length: %d", batchID, len(req.Data))

	return &pb.WorkerResponse{
		Success: true,
	}, nil
}

func (w *WorkerNode) CreateIfStorageFolderDoesntExist() {
	if _, err := os.Stat("/data"); os.IsNotExist(err) {
		err := os.Mkdir("/data", 0755)
		if err != nil {
			log.Printf("Error creating /data directory: %v", err)
		}
	}
	if _, err := os.Stat("/data/unstructured_data"); os.IsNotExist(err) {
		err := os.Mkdir("/data/unstructured_data", 0755)
		if err != nil {
			log.Printf("Error creating /data/unstructured_data directory: %v", err)
		}
	}
	if _, err := os.Stat("/data/streaming_ingestion"); os.IsNotExist(err) {
		err := os.Mkdir("/data/streaming_ingestion", 0755)
		if err != nil {
			log.Printf("Error creating /data/streaming_ingestion directory: %v", err)
		}
		if _, err := os.Stat("/data/batch_ingestion"); os.IsNotExist(err) {
			err := os.Mkdir("/data/batch_ingestion", 0755)
			if err != nil {
				log.Printf("Error creating /data/batch_ingestion directory: %v", err)
			}
		}
	}
}

func (w *WorkerNode) SaveBatch(batchID string, format FileFormat) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	dataToSave := w.ReceivedBatches[batchID]

	switch format {
	case FormatBinary:
		return w.saveUnstructuredData(batchID, dataToSave)
	case FormatAvro:
		return w.saveAvroFile(batchID, dataToSave)
	// case FormatParquet:
	// 	return saveParquetFile(dataToSave)
	default:
		return fmt.Errorf("unsupported file format: %v", format)
	}
}

func (w *WorkerNode) saveUnstructuredData(batchID string, data interface{}) error {
	f, err := os.Create(batchID + ".bin")
	if err != nil {
		log.Printf("Error creating binary file: %v", err)
		return fmt.Errorf("failed to create binary file: %w", err)
	}
	defer f.Close()

	err = binary.Write(f, binary.LittleEndian, data)
	if err != nil {
		log.Printf("Error writing to binary file: %v", err)
		return fmt.Errorf("failed to write to binary file: %w", err)
	}
	return nil
}

func (w *WorkerNode) saveAvroFile(batchID string, data []byte) error {
	filePath := fmt.Sprintf("/data/streaming_ingestion/%s.avro", batchID)

	f, err := os.Create(filePath)
	if err != nil {
		log.Printf("Error creating Avro file: %v", err)
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		log.Printf("Error writing to Avro file: %v", err)
		return err
	}

	return nil
}

// // Helper function to save Parquet file
// func saveParquetFile(data interface{}) error {
// 	f, err := os.Create(filePath + ".parquet")
// 	if err != nil {
// 		log.Printf("Error creating Parquet file: %v", err)
// 		return fmt.Errorf("failed to create Parquet file: %w", err)
// 	}
// 	defer f.Close()

// 	// Create a Parquet writer
// 	// Note: This is a simplified example and will need to be customized
// 	// based on your specific data structure
// 	writer := parquet.NewWriter(f)
// 	defer writer.Close()

// 	// You'll need to convert your data to a format compatible with Parquet
// 	// This is a placeholder and should be replaced with actual conversion logic
// 	row := parquet.NewRow(
// 		parquet.NewBoolColumn("data"),
// 	)

// 	if err := writer.Write(row); err != nil {
// 		log.Printf("Error writing to Parquet file: %v", err)
// 		return fmt.Errorf("failed to write to Parquet file: %w", err)
// 	}
// 	return nil
// }

func (w *WorkerNode) GetWorkerID(ctx context.Context, req *pb.WorkerIDRequest) (*pb.WorkerIDResponse, error) {
	log.Printf("Received GetWorkerID request")
	return &pb.WorkerIDResponse{
		WorkerId: &pb.UUID{Value: w.ID},
	}, nil
}

func (wn *WorkerNode) RetrieveBatchForClient(ctx context.Context, req *pb.GetClientRequestToWorker) (*pb.WorkerBatchResponse, error) {
	batchID := req.BatchId
	batchData := wn.ReceivedBatches[batchID]

	return &pb.WorkerBatchResponse{
		BatchData: batchData,
	}, nil
}
