package fairq

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Monitor struct {
	Context  *context.Context
	DB       *sql.DB
	Interval time.Duration
}

func NewMonitor(connString string, interval time.Duration) (*Monitor, error) {
	ctx := context.Background()
	mon := &Monitor{
		Context:  &ctx,
		Interval: interval,
	}

	if connString == "" {
		return nil, fmt.Errorf("connection string is empty or invalid")
	}

	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	mon.DB = db

	return mon, nil
}

func (m *Monitor) RunMonitor() {
	ctx := *m.Context

	ticker := time.NewTicker(m.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			stats, err := m.RenderQueueStats()
			if err != nil {
				fmt.Println("error rendering queue stats:", err)
			}

			// clear stdout first and then print
			fmt.Print("\033[H\033[2J")
			fmt.Println(stats)
		case <-ctx.Done():
			m.Close()
			return
		}
	}
}

func (m *Monitor) Close() {
	m.DB.Close()
}

func (m *Monitor) RenderQueueStats() (string, error) {
	query := `select status, count(*) from tasks group by status;`

	rows, err := m.DB.Query(query)
	if err != nil {
		return "", err
	}

	defer rows.Close()

	var status string
	var count int
	var stats string

	for rows.Next() {
		err := rows.Scan(&status, &count)
		if err != nil {
			return "", err
		}

		stats += fmt.Sprintf("%s: %d\n", status, count)
	}

	return stats, nil
}
