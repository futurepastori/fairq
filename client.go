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
	dsn string
}

func (c *Client) NewClient(opts NewClientOpts) (*Client, error) {
	db, err := sql.Open("postgres", opts.dsn)

	if err != nil {
		return nil, fmt.Errorf("error creating fairq client: %w", err)
	}

	client := &Client{
		db: db,
	}

	return client, nil
}

func (c *Client) AddTasks(lookupKey string, tasks []Task) {

}
