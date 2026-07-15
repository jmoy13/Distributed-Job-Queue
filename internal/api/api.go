package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jmoy13/distributed-job-queue/internal/queue"
	"github.com/jmoy13/distributed-job-queue/internal/store"
)

type Server struct {
	q  *queue.Queue
	st *store.Store
}

func New(q *queue.Queue, st *store.Store) *Server {
	return &Server{q: q, st: st}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	return mux
}

type createReq struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Type == "" {
		http.Error(w, `{"error":"invalid body: need type and payload"}`, http.StatusBadRequest)
		return
	}
	j, err := queue.NewJob(req.Type, req.Payload)
	if err != nil {
		http.Error(w, `{"error":"bad payload"}`, http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	if err := s.st.InsertJob(ctx, j); err != nil {
		http.Error(w, `{"error":"db insert failed"}`, http.StatusInternalServerError)
		return
	}
	if err := s.q.Enqueue(ctx, j); err != nil {
		http.Error(w, `{"error":"enqueue failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"id": j.ID, "status": "queued"})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	rec, err := s.st.GetJob(r.Context(), r.PathValue("id"))
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, `{"error":"db error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rec)
}
