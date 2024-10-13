package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func generateTasks(batchSize int) (tasks []Task) {
	for i := 0; i < batchSize; i++ {
		tasks = append(tasks, Task{
			Payload: fmt.Sprintf(`{"id": "task%d"}`, i),
		})
	}

	return tasks
}

func main() {
	connStr := "user=victor dbname=fairq sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	lookupKey := os.Getenv("LOOKUP_KEY")
	mode := os.Getenv("MODE")

	switch mode {
	case "insert":
		tasks := generateTasks(100)

		err := CreateTasks(db, lookupKey, tasks)
		if err != nil {
			log.Fatalln("error creating tasks:", err)
		}

		fmt.Printf("added %d tasks", len(tasks))
	case "worker":
		srv, err := NewServer(connStr, nil)
		if err != nil {
			log.Fatal(err)
		}

		srv.RunHandler(func(ctx context.Context, task *Task) error {
			if rand.Float32() < 0.2 {
				err := fmt.Errorf("dummy error from handler run")

				log.Println(err)
				return err
			} else {
				fmt.Println(time.Now().Format(time.DateTime), task.TaskGroupID, task.Status, task.Payload)
				return nil
			}
		})

	case "monitor":
		mon, err := NewMonitor(connStr, 5*time.Second)
		if err != nil {
			log.Fatal(err)
		}

		mon.RunMonitor()

	default:
		log.Fatal("invalid mode: use 'insert' or 'worker'")
	}
}
