package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// MCPPath is the endpoint the streamable HTTP transport serves. Mounting on
	// one exact path (rather than on the root) lets a reverse proxy publish the
	// MCP endpoint without also publishing everything else the process serves.
	MCPPath = "/mcp"
	// HealthPath is a liveness endpoint for the streamable HTTP transport. The
	// MCP endpoint itself is not usable as a probe target: an unauthenticated
	// GET without a session is a client error, not a sign the server is down.
	HealthPath = "/healthz"
)

// RunHTTP runs the MCP server with HTTP/SSE transport.
// It handles graceful shutdown on SIGINT and SIGTERM signals.
//
// The MCP specification superseded HTTP+SSE with streamable HTTP; see
// RunStreamableHTTP.
func (s *Server) RunHTTP(ctx context.Context, addr string) error {
	// Create an SSE handler that returns our MCP server for each new session
	handler := mcp.NewSSEHandler(func(_ *http.Request) *mcp.Server {
		return s.mcp
	}, nil)

	return serve(ctx, addr, handler, "http")
}

// RunStreamableHTTP runs the MCP server with the streamable HTTP transport.
// It handles graceful shutdown on SIGINT and SIGTERM signals.
//
// Unlike HTTP+SSE, streamable HTTP needs no standing server-to-client channel:
// each POST carries its own response stream. That matters behind a reverse
// proxy which closes idle streams - dropping the optional GET stream leaves the
// session usable, whereas under HTTP+SSE it strands every later response.
func (s *Server) RunStreamableHTTP(ctx context.Context, addr string) error {
	handler := mcp.NewStreamableHTTPHandler(func(_ *http.Request) *mcp.Server {
		return s.mcp
	}, nil)

	mux := http.NewServeMux()
	mux.Handle(MCPPath, handler)
	mux.HandleFunc(HealthPath, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := io.WriteString(w, "ok"); err != nil {
			slog.Debug("failed to write health response", "error", err)
		}
	})

	return serve(ctx, addr, mux, "streamable-http")
}

// serverContext falls back to a base context for value lookups while keeping
// the request context's own cancellation and deadline. Holding a second
// context is the entire point of the type; see withServerValues.
//
//nolint:containedctx // Deliberate, see above.
type serverContext struct {
	context.Context
	base context.Context
}

// Value returns the request's value for key, falling back to the base context.
func (c serverContext) Value(key any) any {
	if v := c.Context.Value(key); v != nil {
		return v
	}
	return c.base.Value(key)
}

// withServerValues makes each request's context inherit values from base while
// keeping its own cancellation.
//
// Tool calls run on a context derived from the incoming HTTP request, whereas
// the Argo client's metadata (its logger, the Kubernetes client) lives on the
// process-wide context built at startup. Under stdio the two are the same
// context, so this never mattered; under an HTTP transport the request context
// has none of that metadata, and the Argo REST client panics on the missing
// logger rather than returning an error - taking the whole process with it.
func withServerValues(base context.Context, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The request context is inherited, not replaced: only value lookups
		// fall back to base, while cancellation stays with the request.
		ctx := serverContext{Context: r.Context(), base: base}
		handler.ServeHTTP(w, r.WithContext(ctx)) //nolint:contextcheck // see above
	})
}

// serve runs handler on addr until ctx is cancelled or the listener fails,
// then shuts the server down gracefully. transport names the MCP transport for
// log messages.
func serve(ctx context.Context, addr string, handler http.Handler, transport string) error {
	// Requests must see the values on this context, not just the ones the Go
	// HTTP server puts on a request context.
	handler = withServerValues(ctx, handler)

	// Create a context that cancels on SIGINT or SIGTERM
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("starting MCP server", "transport", transport, "addr", addr)

	// Create HTTP server with timeouts to prevent Slowloris attacks
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start HTTP server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	// Wait for shutdown signal or error
	select {
	case <-ctx.Done():
		slog.Info("shutting down HTTP server", "transport", transport)
		//nolint:contextcheck // Use fresh context for graceful shutdown after cancellation
		if err := httpServer.Shutdown(context.Background()); err != nil {
			return err
		}
	case err := <-errChan:
		return err
	}

	slog.Info("MCP server shutdown gracefully", "transport", transport)
	return nil
}
