package main

import (
	"context"
	"database/sql"
	"log"
	"time"
)

type Server struct {
	Context *context.Context
	DB      *sql.DB
	Config  *ServerConfig
}

type ServerConfig struct {
	interval  time.Duration
	batchSize int
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
			interval:  5 * time.Second,
			batchSize: 10,
		}
	}

	srv.Config = config

	return srv, nil
}

func (s *Server) RunHandler(handler HandlerFunc) error {
	ctx := *s.Context

	resultChan := make(chan Task, s.Config.batchSize)

	go s.runTicker(resultChan)

	for {
		select {
		case <-ctx.Done():
			return nil
		case task := <-resultChan:
			handler(ctx, &task)
		}
	}
}

func (s *Server) runTicker(RC chan Task) {
	ctx := *s.Context

	ticker := time.NewTicker(s.Config.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tasks, err := PopTasks(s.DB, s.Config.batchSize)
			if err != nil {
				log.Printf("Error popping tasks: %v", err)
				continue
			}

			for _, task := range tasks {
				RC <- task
			}
		}
	}
}
