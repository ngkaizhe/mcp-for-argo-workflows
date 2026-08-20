package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCtxKey struct{}

// TestWithServerValues_InheritsBaseValues tests that a handler sees values from
// the server context. Tool calls need the Argo client metadata that lives
// there; without it the Argo REST client panics instead of returning an error.
func TestWithServerValues_InheritsBaseValues(t *testing.T) {
	base := context.WithValue(t.Context(), testCtxKey{}, "from-base")

	var got any
	handler := withServerValues(base, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.Context().Value(testCtxKey{})
	}))

	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	assert.Equal(t, "from-base", got)
}

// TestWithServerValues_RequestValueWins tests that a value on the request
// context takes precedence over the same key on the server context.
func TestWithServerValues_RequestValueWins(t *testing.T) {
	base := context.WithValue(t.Context(), testCtxKey{}, "from-base")

	var got any
	handler := withServerValues(base, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.Context().Value(testCtxKey{})
	}))

	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	req = req.WithContext(context.WithValue(req.Context(), testCtxKey{}, "from-request"))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	assert.Equal(t, "from-request", got)
}

// TestWithServerValues_KeepsRequestCancellation tests that the request keeps
// its own cancellation rather than inheriting the server context's lifetime.
func TestWithServerValues_KeepsRequestCancellation(t *testing.T) {
	var got context.Context
	handler := withServerValues(t.Context(), http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.Context()
	}))

	reqCtx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(http.MethodGet, "/mcp", nil).WithContext(reqCtx)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	require.NotNil(t, got)
	require.NoError(t, got.Err())

	cancel()
	assert.ErrorIs(t, got.Err(), context.Canceled)
}
