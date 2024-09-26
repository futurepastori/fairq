package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type TaskGroup struct {
	ID          int64
	LookupKey   string
	MaxAssigned int64
}

type Task struct {
	ID          int64
	CreatedAt   time.Time
	TaskGroupID int64
	Status      string
	Payload     string
}

var offset = 1 << 20
var numTasks int

func init() {
	tasksEnv := os.Getenv("NUM_TASKS")
	if tasksEnv == "" {
		numTasks = 10
	} else {
		parsedTasks, err := strconv.Atoi(tasksEnv)
		if err != nil {
			log.Printf("Invalid TASKS value: %s. Defaulting to 10.", tasksEnv)
			numTasks = 103
		} else {
			numTasks = parsedTasks
		}
	}
}

func main() {
	connStr := "user=victor dbname=fairq sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	lookup := os.Getenv("LOOKUP_KEY")
	mode := os.Getenv("MODE")

	if mode == "insert" {
		var tasks []Task
		for i := 0; i < numTasks; i++ {
			tasks = append(tasks, Task{
				Payload: fmt.Sprintf(`{"hello": "task%d"}`, i),
			})
		}
		err = createTasks(db, lookup, tasks)
		if err != nil {
			log.Fatal("Error creating tasks:", err)
		}
		fmt.Printf("Successfully inserted %d tasks\n", len(tasks))
	} else if mode == "worker" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		interval := 1 * time.Second // Pop tasks every 5 seconds
		tasksPerInterval := 25      // Number of tasks to pop each time

		fmt.Printf("Starting worker. Popping up to %d tasks every %s\n", tasksPerInterval, interval)
		popTasksPeriodically(ctx, db, interval, tasksPerInterval)
	} else {
		log.Fatal("Invalid mode. Use 'insert' or 'worker'")
	}
}

func createTasks(db *sql.DB, lookupKey string, tasks []Task) error {
	// Step 1: Get or create group based on lookup key
	selectQuery := `
		SELECT id, lookup_key, max_assigned
		FROM task_groups
		WHERE lookup_key = $1
		LIMIT 1;
	`

	var taskGroup TaskGroup
	err := db.QueryRow(selectQuery, lookupKey).Scan(&taskGroup.ID, &taskGroup.LookupKey, &taskGroup.MaxAssigned)

	if err == sql.ErrNoRows {
		// Group doesn't exist, so insert a new one
		insertQuery := `
			INSERT INTO task_groups (lookup_key)
			VALUES ($1)
			RETURNING id, lookup_key, max_assigned;
		`
		err = db.QueryRow(insertQuery, lookupKey).Scan(&taskGroup.ID, &taskGroup.LookupKey, &taskGroup.MaxAssigned)
		if err != nil {
			log.Fatal("can't insert new group", err)
			return err
		}
	} else if err != nil {
		log.Fatal("can't get group", err)
		return err
	}

	greatest := taskGroup.MaxAssigned
	var insertValueRows []string

	// Step 2: Add the tasks into a single insert statement
	for idx, task := range tasks {
		taskId := ((taskGroup.MaxAssigned + int64(idx)) * int64(offset)) + taskGroup.ID

		valueRow := fmt.Sprintf("(%d, %d, '%s')", taskId, taskGroup.ID, task.Payload)
		insertValueRows = append(insertValueRows, valueRow)

		greatest += 1
	}

	insertQuery := `
		INSERT INTO
			tasks (id, task_group_id, payload)
		VALUES
			%s;
	`

	insertStatements := strings.Join(insertValueRows, ",\n")

	_, err = db.Exec(fmt.Sprintf(insertQuery, insertStatements))
	if err != nil {
		return err
	}

	updateGroupQuery := `
		UPDATE task_groups
		SET max_assigned = ($2)
		WHERE lookup_key = ($1)
	`

	_, err = db.Exec(updateGroupQuery, lookupKey, greatest)

	if err != nil {
		return err
	}

	return nil
}

func popTasks(db *sql.DB, numTasks int) ([]Task, error) {
	query := `
		WITH target_tasks AS (
			SELECT
				t.id, t.status
			FROM
				tasks t
			WHERE
				status = 'queued'
			ORDER BY t.id ASC
			LIMIT
				COALESCE($1, 10)
			FOR UPDATE SKIP LOCKED
		)
		UPDATE tasks
		SET
			"status" = 'running'
		FROM
			target_tasks
		WHERE
			tasks.id = target_tasks.id
		RETURNING
			tasks.id, tasks.created_at, tasks.task_group_id, tasks.status, tasks.payload;
	`

	var tasks []Task
	rows, err := db.Query(query, numTasks)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var task Task

		err := rows.Scan(&task.ID, &task.CreatedAt, &task.TaskGroupID, &task.Status, &task.Payload)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, task)
	}

	return tasks, nil
}

func popTasksPeriodically(ctx context.Context, db *sql.DB, interval time.Duration, numTasks int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tasks, err := popTasks(db, numTasks)
			if err != nil {
				log.Printf("Error popping tasks: %v", err)
				continue
			}
			fmt.Printf("Popped %d tasks\n", len(tasks))
			for _, task := range tasks {
				fmt.Printf("Task ID: %d, Status: %s, Payload: %s\n", task.ID, task.Status, task.Payload)
			}
		}
	}
}
