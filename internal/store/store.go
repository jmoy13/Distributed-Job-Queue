package store

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmoy13/distributed-job-queue/internal/queue"
)

//go:embed schema.sql
var schema string

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) InsertJob(ctx context.Context, j *queue.Job) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO jobs (id, type, payload, status, attempts, max_retries, created_at)
		 VALUES ($1, $2, $3, 'queued', $4, $5, $6)`,
		j.ID, j.Type, j.Payload, j.Attempts, j.MaxRetries, j.CreatedAt)
	return err
}

func (s *Store) SetStatus(ctx context.Context, id, status, errMsg string, attempts int) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status=$2, error=NULLIF($3,''), attempts=$4, updated_at=now() WHERE id=$1`,
		id, status, errMsg, attempts)
	return err
}

type JobRecord struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts"`
	Error     string `json:"error,omitempty"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func (s *Store) GetJob(ctx context.Context, id string) (*JobRecord, error) {
	var r JobRecord
	var errMsg *string
	err := s.pool.QueryRow(ctx,
		`SELECT id, type, status, attempts, error, created_at::text, updated_at::text
		 FROM jobs WHERE id=$1`, id).
		Scan(&r.ID, &r.Type, &r.Status, &r.Attempts, &errMsg, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if errMsg != nil {
		r.Error = *errMsg
	}
	return &r, nil
}
