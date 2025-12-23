package restapi

import (
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestAPIClientRetry429 tests that 429 (rate limit) errors trigger retries
func TestAPIClientRetry429(t *testing.T) {
	debug := false
	var requestCount int32

	// Setup test server that returns 429 for first 2 requests, then 200
	serverMux := http.NewServeMux()
	serverMux.HandleFunc("/rate-limited", func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		if count <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error": "rate limited"}`))
			if debug {
				log.Printf("Request %d: Returning 429\n", count)
			}
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status": "success"}`))
			if debug {
				log.Printf("Request %d: Returning 200\n", count)
			}
		}
	})

	server := &http.Server{
		Addr:    "127.0.0.1:8084",
		Handler: serverMux,
	}
	go server.ListenAndServe()
	time.Sleep(100 * time.Millisecond) // Let server start
	defer server.Close()

	// Create client with retry enabled
	opt := &apiClientOpt{
		uri:                 "http://127.0.0.1:8084/",
		insecure:            false,
		username:            "",
		password:            "",
		headers:             make(map[string]string),
		timeout:             5,
		idAttribute:         "id",
		copyKeys:            make([]string, 0),
		writeReturnsObject:  false,
		createReturnsObject: false,
		maxRetries:          3,
		minBackoff:          0.05, // 50ms base backoff
		maxBackoff:          0.1,  // 100ms max backoff
		debug:               debug,
	}
	client, err := NewAPIClient(opt)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}

	// Test that retries eventually succeed
	startTime := time.Now()
	res, err := client.sendRequest("GET", "/rate-limited", "")
	duration := time.Since(startTime)

	if err != nil {
		t.Fatalf("Request failed after retries: %s", err)
	}

	if res != `{"status": "success"}` {
		t.Fatalf("Expected success response, got: %s", res)
	}

	finalCount := atomic.LoadInt32(&requestCount)
	if finalCount != 3 {
		t.Fatalf("Expected 3 requests (2 retries + 1 success), got %d", finalCount)
	}

	// Verify some delay occurred due to retries (but not too much)
	if duration < 100*time.Millisecond {
		t.Logf("Warning: Retries completed very quickly (%v), may not be waiting between attempts", duration)
	}
	if duration > 2*time.Second {
		t.Fatalf("Retries took too long (%v), expected under 2 seconds", duration)
	}

	t.Logf("✓ 429 retry test passed: %d requests in %v", finalCount, duration)
}

// TestAPIClientRetry503 tests that 503 (service unavailable) errors trigger retries
func TestAPIClientRetry503(t *testing.T) {
	debug := false
	var requestCount int32

	// Setup test server that returns 503 for first 2 requests, then 200
	serverMux := http.NewServeMux()
	serverMux.HandleFunc("/unavailable", func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		if count <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error": "service unavailable"}`))
			if debug {
				log.Printf("Request %d: Returning 503\n", count)
			}
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status": "success"}`))
			if debug {
				log.Printf("Request %d: Returning 200\n", count)
			}
		}
	})

	server := &http.Server{
		Addr:    "127.0.0.1:8085",
		Handler: serverMux,
	}
	go server.ListenAndServe()
	time.Sleep(100 * time.Millisecond) // Let server start
	defer server.Close()

	// Create client with retry enabled
	opt := &apiClientOpt{
		uri:                 "http://127.0.0.1:8085/",
		insecure:            false,
		username:            "",
		password:            "",
		headers:             make(map[string]string),
		timeout:             5,
		idAttribute:         "id",
		copyKeys:            make([]string, 0),
		writeReturnsObject:  false,
		createReturnsObject: false,
		maxRetries:          3,
		minBackoff:          0.1, // 100ms
		maxBackoff:          0.2, // 200ms
		debug:               debug,
	}
	client, err := NewAPIClient(opt)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}

	// Test that retries eventually succeed
	startTime := time.Now()
	res, err := client.sendRequest("GET", "/unavailable", "")
	duration := time.Since(startTime)

	if err != nil {
		t.Fatalf("Request failed after retries: %s", err)
	}

	if res != `{"status": "success"}` {
		t.Fatalf("Expected success response, got: %s", res)
	}

	finalCount := atomic.LoadInt32(&requestCount)
	if finalCount != 3 {
		t.Fatalf("Expected 3 requests (2 retries + 1 success), got %d", finalCount)
	}

	// Verify exponential backoff occurred
	if duration < 100*time.Millisecond {
		t.Logf("Warning: Retries completed very quickly (%v), may not be using exponential backoff", duration)
	}
	if duration > 2*time.Second {
		t.Fatalf("Retries took too long (%v), expected under 2 seconds", duration)
	}

	t.Logf("✓ 503 retry test passed: %d requests in %v", finalCount, duration)
}

// TestAPIClientRetryMaxRetriesExceeded tests that retries stop after max_retries
func TestAPIClientRetryMaxRetriesExceeded(t *testing.T) {
	debug := false
	var requestCount int32

	// Setup test server that always returns 429
	serverMux := http.NewServeMux()
	serverMux.HandleFunc("/always-limited", func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error": "rate limited"}`))
		if debug {
			log.Printf("Request %d: Returning 429\n", count)
		}
	})

	server := &http.Server{
		Addr:    "127.0.0.1:8086",
		Handler: serverMux,
	}
	go server.ListenAndServe()
	time.Sleep(100 * time.Millisecond) // Let server start
	defer server.Close()

	// Create client with limited retries
	opt := &apiClientOpt{
		uri:                 "http://127.0.0.1:8086/",
		insecure:            false,
		username:            "",
		password:            "",
		headers:             make(map[string]string),
		timeout:             5,
		idAttribute:         "id",
		copyKeys:            make([]string, 0),
		writeReturnsObject:  false,
		createReturnsObject: false,
		maxRetries:          2,    // Allow 2 retries
		minBackoff:          0.05, // 50ms base
		maxBackoff:          0.1,  // 100ms max
		debug:               debug,
	}
	client, err := NewAPIClient(opt)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}

	// Test that retries stop after max_retries
	startTime := time.Now()
	_, err = client.sendRequest("GET", "/always-limited", "")
	duration := time.Since(startTime)

	// Should eventually fail after exhausting retries
	if err == nil {
		t.Fatalf("Expected error after exhausting retries, but request succeeded")
	}

	if !strings.Contains(err.Error(), "429") {
		t.Fatalf("Expected 429 error, got: %s", err.Error())
	}

	finalCount := atomic.LoadInt32(&requestCount)
	// With maxRetries=2, we get: 1 initial attempt + 2 retries = 3 total requests
	if finalCount != 3 {
		t.Fatalf("Expected 3 requests (1 initial + 2 retries), got %d", finalCount)
	}

	if duration > 1*time.Second {
		t.Fatalf("Retries took too long (%v), expected under 1 second", duration)
	}

	t.Logf("✓ Max retries test passed: %d requests in %v, correctly failed after exhausting retries", finalCount, duration)
}

// TestAPIClientNoRetryOn4xx tests that 4xx errors (except 429) don't trigger retries
func TestAPIClientNoRetryOn4xx(t *testing.T) {
	debug := false
	var requestCount int32

	// Setup test server that returns 404
	serverMux := http.NewServeMux()
	serverMux.HandleFunc("/not-found", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error": "not found"}`))
	})

	server := &http.Server{
		Addr:    "127.0.0.1:8087",
		Handler: serverMux,
	}
	go server.ListenAndServe()
	time.Sleep(100 * time.Millisecond) // Let server start
	defer server.Close()

	// Create client with retry enabled
	opt := &apiClientOpt{
		uri:                 "http://127.0.0.1:8087/",
		insecure:            false,
		username:            "",
		password:            "",
		headers:             make(map[string]string),
		timeout:             5,
		idAttribute:         "id",
		copyKeys:            make([]string, 0),
		writeReturnsObject:  false,
		createReturnsObject: false,
		maxRetries:          3,
		minBackoff:          0.1,
		maxBackoff:          0.2,
		debug:               debug,
	}
	client, err := NewAPIClient(opt)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}

	// Test that 404 doesn't retry
	startTime := time.Now()
	_, err = client.sendRequest("GET", "/not-found", "")
	duration := time.Since(startTime)

	if err == nil {
		t.Fatalf("Expected error for 404, but request succeeded")
	}

	finalCount := atomic.LoadInt32(&requestCount)
	// Should only make 1 request, no retries for 404
	if finalCount != 1 {
		t.Fatalf("Expected 1 request (no retries for 404), got %d", finalCount)
	}

	// Should fail fast without retry delays
	if duration > 2*time.Second {
		t.Fatalf("Request took too long (%v), should fail fast without retries", duration)
	}

	t.Logf("✓ No retry on 4xx test passed: %d request in %v, correctly didn't retry", finalCount, duration)
}

// TestAPIClientRetryDisabled tests that when max_retries is 0, no retries occur
func TestAPIClientRetryDisabled(t *testing.T) {
	debug := false
	var requestCount int32

	// Setup test server that returns 429
	serverMux := http.NewServeMux()
	serverMux.HandleFunc("/rate-limited", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error": "rate limited"}`))
	})

	server := &http.Server{
		Addr:    "127.0.0.1:8088",
		Handler: serverMux,
	}
	go server.ListenAndServe()
	time.Sleep(100 * time.Millisecond) // Let server start
	defer server.Close()

	// Create client with retries DISABLED (maxRetries = 0)
	opt := &apiClientOpt{
		uri:                 "http://127.0.0.1:8088/",
		insecure:            false,
		username:            "",
		password:            "",
		headers:             make(map[string]string),
		timeout:             5,
		idAttribute:         "id",
		copyKeys:            make([]string, 0),
		writeReturnsObject:  false,
		createReturnsObject: false,
		maxRetries:          0, // Retries disabled
		minBackoff:          0.1,
		maxBackoff:          30.0,
		debug:               debug,
	}
	client, err := NewAPIClient(opt)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}

	// Test that no retries occur
	startTime := time.Now()
	_, err = client.sendRequest("GET", "/rate-limited", "")
	duration := time.Since(startTime)

	if err == nil {
		t.Fatalf("Expected error for 429, but request succeeded")
	}

	finalCount := atomic.LoadInt32(&requestCount)
	// Should only make 1 request, no retries
	if finalCount != 1 {
		t.Fatalf("Expected 1 request (retries disabled), got %d", finalCount)
	}

	// Should fail fast without retry delays
	if duration > 1*time.Second {
		t.Fatalf("Request took too long (%v), should fail immediately with retries disabled", duration)
	}

	t.Logf("✓ Retry disabled test passed: %d request in %v, correctly no retries", finalCount, duration)
}
