package restapi

import (
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestGet429BackoffTime_NilHeaders(t *testing.T) {
	duration, found := Get429BackoffTime(nil)
	if found {
		t.Error("Expected found=false for nil headers")
	}
	if duration != 0 {
		t.Errorf("Expected duration=0 for nil headers, got %v", duration)
	}
}

func TestGet429BackoffTime_EmptyHeaders(t *testing.T) {
	headers := http.Header{}
	duration, found := Get429BackoffTime(headers)
	if found {
		t.Error("Expected found=false for empty headers")
	}
	if duration != 0 {
		t.Errorf("Expected duration=0 for empty headers, got %v", duration)
	}
}

func TestGet429BackoffTime_RetryAfter_Seconds(t *testing.T) {
	tests := []struct {
		name          string
		retryAfterVal string
		expectedFound bool
		expectedDur   time.Duration
	}{
		{"10 seconds", "10", true, 10 * time.Second},
		{"60 seconds", "60", true, 60 * time.Second},
		{"1 second", "1", true, 1 * time.Second},
		{"120 seconds", "120", true, 120 * time.Second},
		{"0 seconds", "0", true, 0 * time.Second}, // 0 means retry immediately
		{"invalid non-numeric", "abc", false, 0},
		{"negative seconds", "-10", true, 0 * time.Second},                          // Negative normalized to 0 (retry immediately)
		{"fractional seconds", "10.5", true, 10*time.Second + 500*time.Millisecond}, // Go ParseDuration accepts fractionals
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := http.Header{}
			headers.Set("Retry-After", tt.retryAfterVal)

			duration, found := Get429BackoffTime(headers)

			if found != tt.expectedFound {
				t.Errorf("Expected found=%v, got %v", tt.expectedFound, found)
			}
			if duration != tt.expectedDur {
				t.Errorf("Expected duration=%v, got %v", tt.expectedDur, duration)
			}
		})
	}
}

func TestGet429BackoffTime_RetryAfter_HTTPDate(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func() string
		expectedFound bool
		minDuration   time.Duration
		maxDuration   time.Duration
	}{
		{
			name: "Future time 30 seconds from now",
			setupFunc: func() string {
				futureTime := time.Now().Add(30 * time.Second)
				return futureTime.UTC().Format(http.TimeFormat)
			},
			expectedFound: true,
			minDuration:   29 * time.Second,
			maxDuration:   31 * time.Second,
		},
		{
			name: "Future time 2 minutes from now",
			setupFunc: func() string {
				futureTime := time.Now().Add(2 * time.Minute)
				return futureTime.UTC().Format(http.TimeFormat)
			},
			expectedFound: true,
			minDuration:   119 * time.Second,
			maxDuration:   121 * time.Second,
		},
		{
			name: "Past time",
			setupFunc: func() string {
				pastTime := time.Now().Add(-10 * time.Second)
				return pastTime.UTC().Format(http.TimeFormat)
			},
			expectedFound: true,
			minDuration:   0,
			maxDuration:   0,
		},
		{
			name: "Invalid date format",
			setupFunc: func() string {
				return "Not a valid date"
			},
			expectedFound: false,
			minDuration:   0,
			maxDuration:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := http.Header{}
			headers.Set("Retry-After", tt.setupFunc())

			duration, found := Get429BackoffTime(headers)

			if found != tt.expectedFound {
				t.Errorf("Expected found=%v, got %v", tt.expectedFound, found)
			}
			if tt.expectedFound && (duration < tt.minDuration || duration > tt.maxDuration) {
				t.Errorf("Expected duration between %v and %v, got %v", tt.minDuration, tt.maxDuration, duration)
			}
		})
	}
}

func TestGet429BackoffTime_XRateLimitReset_WithDate(t *testing.T) {
	tests := []struct {
		name        string
		headerName  string
		setupFunc   func() (resetTimestamp string, dateHeader string)
		expectedMin time.Duration
		expectedMax time.Duration
	}{
		{
			name:       "X-Rate-Limit-Reset with Date header 60s future",
			headerName: "X-Rate-Limit-Reset",
			setupFunc: func() (string, string) {
				now := time.Now()
				resetTime := now.Add(60 * time.Second)
				return strconv.FormatInt(resetTime.Unix(), 10), now.UTC().Format(http.TimeFormat)
			},
			expectedMin: 59 * time.Second,
			expectedMax: 62 * time.Second,
		},
		{
			name:       "X-RateLimit-Reset variant",
			headerName: "X-RateLimit-Reset",
			setupFunc: func() (string, string) {
				now := time.Now()
				resetTime := now.Add(45 * time.Second)
				return strconv.FormatInt(resetTime.Unix(), 10), now.UTC().Format(http.TimeFormat)
			},
			expectedMin: 44 * time.Second,
			expectedMax: 47 * time.Second,
		},
		{
			name:       "RateLimit-Reset variant",
			headerName: "RateLimit-Reset",
			setupFunc: func() (string, string) {
				now := time.Now()
				resetTime := now.Add(90 * time.Second)
				return strconv.FormatInt(resetTime.Unix(), 10), now.UTC().Format(http.TimeFormat)
			},
			expectedMin: 89 * time.Second,
			expectedMax: 92 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetTimestamp, dateHeader := tt.setupFunc()

			headers := http.Header{}
			headers.Set(tt.headerName, resetTimestamp)
			headers.Set("Date", dateHeader)

			duration, found := Get429BackoffTime(headers)

			if !found {
				t.Error("Expected to find valid backoff time")
			}
			if duration < tt.expectedMin || duration > tt.expectedMax {
				t.Errorf("Expected duration between %v and %v, got %v", tt.expectedMin, tt.expectedMax, duration)
			}
		})
	}
}

func TestGet429BackoffTime_XRateLimitReset_WithoutDate(t *testing.T) {
	tests := []struct {
		name        string
		headerName  string
		futureDelay time.Duration
	}{
		{"X-Rate-Limit-Reset 30s future", "X-Rate-Limit-Reset", 30 * time.Second},
		{"X-RateLimit-Reset 45s future", "X-RateLimit-Reset", 45 * time.Second},
		{"RateLimit-Reset 60s future", "RateLimit-Reset", 60 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			futureTime := time.Now().Add(tt.futureDelay)

			headers := http.Header{}
			headers.Set(tt.headerName, strconv.FormatInt(futureTime.Unix(), 10))

			duration, found := Get429BackoffTime(headers)

			if !found {
				t.Error("Expected to find valid backoff time")
			}

			// Allow 2 second tolerance for test execution time
			minDuration := tt.futureDelay - 2*time.Second
			maxDuration := tt.futureDelay + 2*time.Second
			if duration < minDuration || duration > maxDuration {
				t.Errorf("Expected duration between %v and %v, got %v", minDuration, maxDuration, duration)
			}
		})
	}
}

func TestGet429BackoffTime_XRateLimitReset_PastTime(t *testing.T) {
	headers := http.Header{}
	pastTime := time.Now().Add(-30 * time.Second)
	headers.Set("X-Rate-Limit-Reset", strconv.FormatInt(pastTime.Unix(), 10))

	duration, found := Get429BackoffTime(headers)

	if !found {
		t.Error("Expected found=true for past reset time (immediate retry)")
	}
	if duration != 0 {
		t.Errorf("Expected duration=0 for past reset time, got %v", duration)
	}
}

func TestGet429BackoffTime_XRateLimitReset_InvalidTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		timestamp string
	}{
		{"non-numeric", "abc123"},
		{"empty", ""},
		{"special characters", "!@#$%"},
		{"float", "1234.567"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := http.Header{}
			headers.Set("X-Rate-Limit-Reset", tt.timestamp)

			duration, found := Get429BackoffTime(headers)

			if found {
				t.Error("Expected found=false for invalid timestamp")
			}
			if duration != 0 {
				t.Errorf("Expected duration=0 for invalid timestamp, got %v", duration)
			}
		})
	}
}

func TestGet429BackoffTime_HeaderPriority(t *testing.T) {
	// Retry-After should take precedence over X-Rate-Limit-Reset
	headers := http.Header{}
	headers.Set("Retry-After", "10")
	futureTime := time.Now().Add(60 * time.Second)
	headers.Set("X-Rate-Limit-Reset", strconv.FormatInt(futureTime.Unix(), 10))

	duration, found := Get429BackoffTime(headers)

	if !found {
		t.Error("Expected to find valid backoff time")
	}
	// Should use Retry-After (10s), not X-Rate-Limit-Reset (60s)
	if duration < 9*time.Second || duration > 11*time.Second {
		t.Errorf("Expected duration around 10s (from Retry-After), got %v", duration)
	}
}

func TestGet429BackoffTime_MultipleResetHeaders(t *testing.T) {
	// X-Rate-Limit-Reset should take precedence over other variants
	headers := http.Header{}
	futureTime1 := time.Now().Add(30 * time.Second)
	futureTime2 := time.Now().Add(60 * time.Second)
	futureTime3 := time.Now().Add(90 * time.Second)

	headers.Set("RateLimit-Reset", strconv.FormatInt(futureTime3.Unix(), 10))
	headers.Set("X-RateLimit-Reset", strconv.FormatInt(futureTime2.Unix(), 10))
	headers.Set("X-Rate-Limit-Reset", strconv.FormatInt(futureTime1.Unix(), 10))

	duration, found := Get429BackoffTime(headers)

	if !found {
		t.Error("Expected to find valid backoff time")
	}
	// Should use X-Rate-Limit-Reset (30s), the first in priority order
	if duration < 28*time.Second || duration > 32*time.Second {
		t.Errorf("Expected duration around 30s (from X-Rate-Limit-Reset), got %v", duration)
	}
}

func TestGet429BackoffTime_DateHeaderParsing(t *testing.T) {
	tests := []struct {
		name       string
		dateHeader string
		shouldWork bool
	}{
		{"Valid RFC1123 format", time.Now().UTC().Format(http.TimeFormat), true},
		{"Invalid date format", "Not a date", false},
		{"Empty date", "", false},
		{"Random string", "2024-01-01 00:00:00", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var serverTime time.Time
			if tt.shouldWork {
				serverTime = time.Now()
			} else {
				serverTime = time.Now().Add(-10 * time.Second)
			}

			resetTime := serverTime.Add(60 * time.Second)

			headers := http.Header{}
			headers.Set("X-Rate-Limit-Reset", strconv.FormatInt(resetTime.Unix(), 10))
			headers.Set("Date", tt.dateHeader)

			duration, found := Get429BackoffTime(headers)

			// Should still find time even with invalid Date header (falls back to local time)
			if !found {
				t.Error("Expected to find valid backoff time")
			}
			if duration <= 0 {
				t.Errorf("Expected positive duration, got %v", duration)
			}
		})
	}
}

func TestGet429BackoffTime_RealWorldExamples(t *testing.T) {
	t.Run("GitHub API style", func(t *testing.T) {
		now := time.Now()
		resetTime := now.Add(60 * time.Second)

		headers := http.Header{}
		headers.Set("X-RateLimit-Reset", strconv.FormatInt(resetTime.Unix(), 10))
		headers.Set("X-RateLimit-Remaining", "0")
		headers.Set("Date", now.UTC().Format(http.TimeFormat))

		duration, found := Get429BackoffTime(headers)
		if !found {
			t.Error("Expected to parse GitHub-style headers")
		}
		if duration < 59*time.Second || duration > 62*time.Second {
			t.Errorf("Expected ~60s backoff, got %v", duration)
		}
	})

	t.Run("Cloudflare style", func(t *testing.T) {
		headers := http.Header{}
		headers.Set("Retry-After", "30")

		duration, found := Get429BackoffTime(headers)
		if !found {
			t.Error("Expected to parse Cloudflare-style headers")
		}
		if duration != 30*time.Second {
			t.Errorf("Expected 30s backoff, got %v", duration)
		}
	})

	t.Run("AWS API Gateway style", func(t *testing.T) {
		futureTime := time.Now().Add(120 * time.Second)

		headers := http.Header{}
		headers.Set("Retry-After", futureTime.UTC().Format(http.TimeFormat))

		duration, found := Get429BackoffTime(headers)
		if !found {
			t.Error("Expected to parse AWS-style headers")
		}
		if duration < 118*time.Second || duration > 122*time.Second {
			t.Errorf("Expected ~120s backoff, got %v", duration)
		}
	})

	t.Run("Okta style with Date header", func(t *testing.T) {
		serverTime := time.Now()
		resetTime := serverTime.Add(45 * time.Second)

		headers := http.Header{}
		headers.Set("X-Rate-Limit-Reset", strconv.FormatInt(resetTime.Unix(), 10))
		headers.Set("Date", serverTime.UTC().Format(http.TimeFormat))

		duration, found := Get429BackoffTime(headers)
		if !found {
			t.Error("Expected to parse Okta-style headers")
		}
		if duration < 44*time.Second || duration > 47*time.Second {
			t.Errorf("Expected ~45s backoff, got %v", duration)
		}
	})
}

func TestGet429BackoffTime_EdgeCases(t *testing.T) {
	t.Run("Zero timestamp", func(t *testing.T) {
		headers := http.Header{}
		headers.Set("X-Rate-Limit-Reset", "0")

		duration, found := Get429BackoffTime(headers)
		if !found {
			t.Error("Expected found=true for zero timestamp (immediate retry)")
		}
		if duration != 0 {
			t.Errorf("Expected duration=0, got %v", duration)
		}
	})

	t.Run("Very large timestamp", func(t *testing.T) {
		headers := http.Header{}
		// Year 2100
		headers.Set("X-Rate-Limit-Reset", "4102444800")

		duration, found := Get429BackoffTime(headers)
		if !found {
			t.Error("Expected to find valid backoff time")
		}
		if duration <= 0 {
			t.Errorf("Expected positive duration, got %v", duration)
		}
	})

	t.Run("Mixed case header names", func(t *testing.T) {
		headers := http.Header{}
		headers.Set("retry-after", "15")

		duration, found := Get429BackoffTime(headers)
		if !found {
			t.Error("Expected to find valid backoff time (HTTP headers are case-insensitive)")
		}
		if duration != 15*time.Second {
			t.Errorf("Expected 15s, got %v", duration)
		}
	})

	t.Run("Multiple values for same header", func(t *testing.T) {
		headers := http.Header{}
		headers.Add("Retry-After", "10")
		headers.Add("Retry-After", "20")

		duration, found := Get429BackoffTime(headers)
		if !found {
			t.Error("Expected to find valid backoff time")
		}
		// Should use first value
		if duration != 10*time.Second {
			t.Errorf("Expected 10s (first value), got %v", duration)
		}
	})
}
