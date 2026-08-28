// Command schedsim-go is an alternative to image/server/app.py: the same
// small HTTP API (GET /healthz, POST /run) the schedsim CLI already
// speaks, but backed directly by k8s.io/kubernetes/pkg/scheduler running
// in-process against a fake clientset instead of an unmodified
// kube-scheduler binary talking to a hand-written fake apiserver. See
// internal/simcluster for the actual seam; this file is just the HTTP
// wiring, the same job app.py does.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"schedsim-go/internal/simcluster"
)

// runRequest mirrors image/server/app.py's RunRequest model field for
// field.
type runRequest struct {
	Nodes     []map[string]any `json:"nodes"`
	Workloads []map[string]any `json:"workloads"`
	Timeout   int              `json:"timeout"`
	Keep      bool             `json:"keep"`
	Progress  bool             `json:"progress"`
}

func main() {
	addr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cluster, err := simcluster.New(ctx)
	if err != nil {
		log.Fatalf("starting scheduler: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		// The scheduler and its informers run in this same process, so
		// answering this at all means the control plane is up -- same
		// reasoning as app.py's healthz.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("POST /run", handleRun(cluster))

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("schedsim-go listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// handleRun streams newline-delimited JSON progress events, the same
// contract apiclient.py's run() expects: zero or more progress lines
// while the run is in flight, then exactly one {"phase": "done", ...} or
// {"phase": "error", ...} line to close the stream.
func handleRun(cluster *simcluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req runRequest
		req.Timeout, req.Progress = 30, true // defaults, same as app.py's RunRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		bw := bufio.NewWriter(w)
		flusher, _ := w.(http.Flusher)
		writeLine := func(event simcluster.Event) {
			line, err := json.Marshal(event)
			if err != nil {
				return
			}
			line = append(line, '\n')
			_, _ = bw.Write(line)
			_ = bw.Flush()
			if flusher != nil {
				flusher.Flush()
			}
		}

		// The run itself is driven to completion on a context independent
		// of the request -- a client disconnect stops us writing to it,
		// but (like simulate.run()'s `finally`, per app.py's docstring)
		// cleanup still runs against the cluster either way. events is
		// always drained to its close here so Run never blocks on a send
		// nobody is reading.
		events := make(chan simcluster.Event, 8)
		type result struct {
			rows []simcluster.Row
			err  error
		}
		resultCh := make(chan result, 1)
		go func() {
			rows, err := cluster.Run(context.Background(), req.Nodes, req.Workloads,
				time.Duration(req.Timeout)*time.Second, req.Keep, events)
			resultCh <- result{rows, err}
		}()

		for event := range events {
			phase, _ := event["phase"].(string)
			if req.Progress || phase == "done" || phase == "error" {
				writeLine(event)
			}
		}
		if res := <-resultCh; res.err != nil {
			// Any failure -- a reported SimError or a bug -- becomes a
			// terminal line instead of a silently truncated stream, same
			// as app.py's except clause around simulate.run().
			writeLine(simcluster.Event{"phase": "error", "message": res.err.Error()})
		}
	}
}
