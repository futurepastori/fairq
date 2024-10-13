package fairq

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lib/pq"
)

type Server struct {
	Context *context.Context
	DB      *sql.DB
	Config  *ServerConfig

	Flush Flush
}

type Flush struct {
	FlushableTasks chan *Task
	mu             sync.Mutex
}

type ServerConfig struct {
	Interval  time.Duration
	BatchSize int
}

type HandlerFunc func(ctx context.Context, task *Task) error

func NewServer(connString string, config *ServerConfig) (*Server, error) {
	ctx := context.Background()
	srv := &Server{
		Context: &ctx,
	}

	if connString == "" {
		log.Fatal("connection string is empty or invalid")
	}

	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	srv.DB = db

	if config == nil {
		config = &ServerConfig{
			Interval:  5 * time.Second,
			BatchSize: 10,
		}
	}

	srv.Config = config

	srv.Flush.FlushableTasks = make(chan *Task, srv.Config.BatchSize)

	return srv, nil
}

func (s *Server) RunHandler(handler HandlerFunc) error {
	ctx := *s.Context

	resultChan := make(chan *Task, s.Config.BatchSize)

	go s.initTicker(resultChan)
	go s.initFlusher()

	for {
		select {
		case <-ctx.Done():
			return nil
		case task := <-resultChan:
			err := handler(ctx, task)

			if err != nil {
				err := updateTaskAsFailed(s.DB, task)

				if err != nil {
					log.Println(err)
				}
			} else {
				s.DiscardTask(task)
			}
		}
	}
}

func (s *Server) initTicker(RC chan *Task) {
	ctx := *s.Context

	ticker := time.NewTicker(s.Config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println(time.Now(), "context must be done")

			return
		case <-ticker.C:
			tasks, err := PopTasks(s.DB, s.Config.BatchSize)

			log.Println(time.Now(), fmt.Sprintf("popped %d tasks", len(tasks)))

			if err != nil {
				log.Printf("Error popping tasks: %v", err)
				continue
			}

			for _, task := range tasks {
				RC <- &task
			}
		}
	}
}

func (s *Server) initFlusher() {
	const flushBatchSize = 1000
	var flushBatch []*Task

	for {
		select {
		case task := <-s.Flush.FlushableTasks:
			flushBatch = append(flushBatch, task)

			if len(flushBatch) >= flushBatchSize {
				s.flushDiscarded(&flushBatch)

				flushBatch = []*Task{}
			}

		case <-time.After(2 * time.Second):
			s.flushDiscarded(&flushBatch)

			flushBatch = []*Task{}
		}
	}
}

func (s *Server) flushDiscarded(flushBatch *([]*Task)) {
	isLocked := s.Flush.mu.TryLock()
	if !isLocked {
		return
	}

	defer s.Flush.mu.Unlock()

	if len(*flushBatch) == 0 {
		return
	}

	taskIds := make([]int64, len(*flushBatch))
	for idx, task := range *flushBatch {
		taskIds[idx] = task.ID
	}

	query := `
		UPDATE tasks
		SET status = 'completed'
		WHERE id = ANY($1)
	`

	if _, err := s.DB.Exec(query, pq.Array(taskIds)); err != nil {
		log.Println(time.Now().Format(time.DateTime), fmt.Errorf("error updating tasks as completed to db: %w", err))
	}
}

func (s *Server) DiscardTask(task *Task) {
	s.Flush.FlushableTasks <- task
}

func updateTaskAsFailed(db *sql.DB, task *Task) error {
	query := `
		UPDATE tasks
		SET status = 'failed'
		WHERE id = $1
	`

	if _, err := db.Exec(query, task.ID); err != nil {
		return fmt.Errorf("couldn't update failed task status: %w", err)
	}

	return nil
}
