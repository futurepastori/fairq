package fairq

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Client struct {
	db *sql.DB
}

type NewClientOpts struct {
	Dsn string
}

func NewClient(opts NewClientOpts) (*Client, error) {
	db, err := sql.Open("postgres", opts.Dsn)

	if err != nil {
		return nil, fmt.Errorf("error creating fairq client: %w", err)
	}

	client := &Client{
		db: db,
	}

	return client, nil
}

func (c *Client) Close() error {
	return c.db.Close()
}

func (c *Client) CreateTasks(lookupKey string, tasks []Task) error {
	return CreateTasks(c.db, lookupKey, tasks)
}
