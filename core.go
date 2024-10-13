package asynq

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
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

// Insert task groups are locked by lookup key on sortable ID generation
// before batch inserting to db
type SafeTaskGroup struct {
	mu        sync.Mutex
	taskGroup TaskGroup
}

var safeTaskGroupLocks sync.Map

// Get or create lockable task group by lookup key from map and cast
func getTaskGroupLock(lookupKey string) *SafeTaskGroup {
	lockValue, _ := safeTaskGroupLocks.LoadOrStore(lookupKey, &SafeTaskGroup{
		taskGroup: TaskGroup{
			LookupKey: lookupKey,
		},
	})

	lock := lockValue.(*SafeTaskGroup)

	return lock
}

// insert a new task group with index 1
func (s *SafeTaskGroup) createGroup(db *sql.DB) error {
	query := `
		INSERT INTO task_groups (lookup_key)
		VALUES ($1)
		RETURNING id, lookup_key, max_assigned;
	`

	err := db.QueryRow(query, s.taskGroup.LookupKey).Scan(
		&s.taskGroup.ID,
		&s.taskGroup.LookupKey,
		&s.taskGroup.MaxAssigned,
	)

	return err
}

// Refresh max assigned task index from a certain task group from db,
// or call to create a new one
func (s *SafeTaskGroup) refreshOrCreateGroup(db *sql.DB) error {
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
		err := s.createGroup(db)
		if err != nil {
			return fmt.Errorf("can't create group: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("can't refresh group: %w", err)
	}

	return nil
}

// Generate SQL for bulk importing tasks with evenly distributed IDs.
// For a task t_i in batch T belonging to group G_j, calculate its ID as:
//
//	ID = previous_ID + i * C + j
//
// where C is a constant offset (default 2^20) defining the max number of lookup keys.
//
// Example:
//   - Alice and Bob each add 10 tasks.
//   - We allow up to 10 users (offset: 10 lookup keys).
//
// Therefore:
//   - Alice's task IDs: 1, 11, 21, 31, 41, 51, 61, 71, 81, 91
//   - Bob's task IDs:   2, 12, 22, 32, 42, 52, 62, 72, 82, 92
//   - ID slots 3 to 10 are reserved for the first task of other users.
//   - When popping a batch of 10 tasks ordered by ascending ID, tasks will alternate between users:
//     {A, B, A, B, A, B, ...}, creating a round-robin effect.
func (s *SafeTaskGroup) composeInsertQuery(tasks *[]Task) string {
	query := `
		INSERT INTO
				tasks (id, task_group_id, payload, status)
		VALUES
				%s;
	`

	insertValueRows := make([]string, len(*tasks))
	for idx, task := range *tasks {
		taskId := ((s.taskGroup.MaxAssigned + int64(idx) + 1) * GROUP_OFFSET) + s.taskGroup.ID

		insertRow := fmt.Sprintf("(%d, %d, '%s', 'queued')", taskId, s.taskGroup.ID, task.Payload)
		insertValueRows[idx] = insertRow
	}

	return fmt.Sprintf(query, strings.Join(insertValueRows, ",\n"))
}

func (s *SafeTaskGroup) updateMaxAssigned(db *sql.DB, maxAssigned int64) error {
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

// Locks the task group, refreshes max assigned id from database, generates query
// batch inserts task, and updates the latest assigned index to db on sync before unlocking
func CreateTasks(db *sql.DB, lookupKey string, tasks []Task) error {
	stg := getTaskGroupLock(lookupKey)

	stg.mu.Lock()
	defer stg.mu.Unlock()

	err := stg.refreshOrCreateGroup(db)
	if err != nil {
		return err
	}

	insertQuery := stg.composeInsertQuery(&tasks)

	_, err = db.Exec(insertQuery)
	if err != nil {
		return fmt.Errorf("error inserting tasks to db: %w", err)
	}

	err = stg.updateMaxAssigned(db, stg.taskGroup.MaxAssigned+int64(len(tasks)))

	if err != nil {
		return err
	}

	return nil
}

// Returns a slice of tasks from the DB.
//
// Queries for a batch of size batchSize ordered by ID asc, ensuring tasks are only
// picked once using FOR UPDATE SKIP LOCKED.
//
// On query, it updates selected tasks' status to 'running'.
func PopTasks(db *sql.DB, batchSize int) ([]Task, error) {
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
	rows, err := db.Query(query, batchSize)

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
