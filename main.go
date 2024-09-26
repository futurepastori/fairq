package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

const GROUP_OFFSET = 1 << 20

type Task struct {
	ID          int64
	CreatedAt   time.Time
	TaskGroupID int64
	Status      string
	Payload     string
}

type TaskGroup struct {
	ID          int64
	LookupKey   string
	MaxAssigned int64
}

type SafeGroup struct {
	mu        sync.Mutex
	taskGroup TaskGroup
}

var safeGroupLocks sync.Map

func (s *SafeGroup) RefreshGroup(db *sql.DB) error {
	query := `
		SELECT id, lookup_key, max_assigned
		FROM task_groups
		WHERE lookup_key = $1
		LIMIT 1;
  `
	err := db.QueryRow(query, s.taskGroup.LookupKey).Scan(
		&s.taskGroup.ID,
		&s.taskGroup.LookupKey,
		&s.taskGroup.MaxAssigned,
	)

	if errors.Is(err, sql.ErrNoRows) {
		query := `
			INSERT INTO task_groups (lookup_key)
			VALUES ($1)
			RETURNING id, lookup_key, max_assigned;
		`
		err = db.QueryRow(query, s.taskGroup.LookupKey).Scan(
			&s.taskGroup.ID,
			&s.taskGroup.LookupKey,
			&s.taskGroup.MaxAssigned,
		)
	}

	if err != nil {
		return fmt.Errorf("can't create or refresh group: %w", err)
	}

	return nil
}

func (s *SafeGroup) UpdateMaxAssigned(db *sql.DB, maxAssigned int64) error {
	query := `
		UPDATE task_groups
		SET max_assigned = $1
		WHERE lookup_key = $2
	`
	_, err := db.Exec(query, maxAssigned, s.taskGroup.LookupKey)

	if err != nil {
		return fmt.Errorf("can't update max assigned task to db: %w", err)
	}

	return nil
}

func CreateTasks(db *sql.DB, lookupKey string, tasks []Task) error {
	lockValue, _ := safeGroupLocks.LoadOrStore(lookupKey, &SafeGroup{
		taskGroup: TaskGroup{
			LookupKey: lookupKey,
		},
	})

	lock := lockValue.(*SafeGroup)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	err := lock.RefreshGroup(db)
	if err != nil {
		return fmt.Errorf("error refreshing group on createtasks: %w", err)
	}

	insertQuery := `
		INSERT INTO
				tasks (id, task_group_id, payload, status)
		VALUES
				%s;
	`
	insertValueRows := make([]string, len(tasks))

	for idx, task := range tasks {
		taskId := ((lock.taskGroup.MaxAssigned + int64(idx) + 1) * GROUP_OFFSET) + lock.taskGroup.ID

		insertRow := fmt.Sprintf("(%d, %d, '%s', 'queued')", taskId, lock.taskGroup.ID, task.Payload)
		insertValueRows[idx] = insertRow
	}

	_, err = db.Exec(fmt.Sprintf(insertQuery, strings.Join(insertValueRows, ",\n")))
	if err != nil {
		return fmt.Errorf("error inserting tasks to db: %w", err)
	}

	newMaxAssigned := lock.taskGroup.MaxAssigned + int64(len(tasks))
	err = lock.UpdateMaxAssigned(db, newMaxAssigned)

	if err != nil {
		return fmt.Errorf("could not update maxAssigned: %w", err)
	}

	return nil
}

func PopTasks(db *sql.DB, numTasks int) ([]Task, error) {
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
			fmt.Println(time.Now().Format(time.DateTime), task.TaskGroupID, task.Status, task.Payload)

			return nil
		})
	default:
		log.Fatal("invalid mode: use 'insert' or 'worker'")
	}
}
