package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/identity_service/db"
	"github.com/razvanmarinn/identity_service/db/models"
	kf "github.com/razvanmarinn/identity_service/kafka"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedVerificationServiceServer
	db *sql.DB
}

func setupRouter(database *sql.DB, kf *kf.KafkaWriter) *gin.Engine {
	r := gin.Default()

	r.POST("/register/", func(c *gin.Context) {
		var user models.Client
		if err := c.ShouldBindJSON(&user); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if err := db.RegisterUser(database, &user); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to register user"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "User registered successfully"})
	})

	r.POST("/project/register/", func(c *gin.Context) {
		var project models.Project
		if err := c.ShouldBindJSON(&project); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if err := db.RegisterProject(database, &project); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to register project"})
			return
		}

		kf.WriteMessageForSchema(context.Background(), project.Name, kafka.Message{
			Key:   []byte(project.Name),
			Value: []byte(fmt.Sprintf("Project %s registered", project.Name)),
		})
		c.JSON(http.StatusOK, gin.H{"message": "Project registered successfully"})
	})

	return r
}

func main() {
	database, err := db.Connect_to_db()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()
	s := grpc.NewServer()
	pb.RegisterVerificationServiceServer(s, &server{db: database})

	var wg sync.WaitGroup
	wg.Add(1)
	lis, err := net.Listen("tcp", ":50056")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	go func() {
		defer wg.Done()
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokersStr == "" {
		fmt.Println("Warning: KAFKA_BROKERS environment variable not set. Defaulting to localhost:9092.")
		kafkaBrokersStr = "localhost:9092"
	}
	kafkaBrokers := strings.Split(kafkaBrokersStr, ",")
	fmt.Printf("Using Kafka brokers: %v\n", kafkaBrokers)

	kafkaWriter := kf.NewKafkaWriter(kafkaBrokers)
	r := setupRouter(database, kafkaWriter)
	r.Run(":8082")
}

func (s *server) VerifyProjectExistence(ctx context.Context, in *pb.VerifyProjectExistenceRequest) (*pb.VerifyProjectExistenceResponse, error) {
	projectName := in.GetProjectName()
	log.Printf("Verifying existence of project: %s", projectName)
	exists, err := db.CheckProjectExistence(s.db, projectName)
	if err != nil {
		return nil, err
	}
	log.Printf("Project %s exists: %v", projectName, exists)
	return &pb.VerifyProjectExistenceResponse{Exists: exists}, nil
}
