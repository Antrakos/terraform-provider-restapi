package restapi

import (
	"net/http"
	"strconv"
	"time"
)

// Get429BackoffTime extracts backoff duration from rate limit response headers.
// Returns the parsed duration and a boolean indicating whether a valid duration was found.
//
// Supports multiple industry-standard formats:
// 1. Retry-After: seconds or HTTP date (RFC 6585)
// 2. X-Rate-Limit-Reset: Unix timestamp with optional Date header (GitHub, Twitter, Okta)
// 3. RateLimit-Reset: Unix timestamp with optional Date header (modern variant)
//
// Special cases:
// - Retry-After: 0 or negative values return (0, true) meaning "retry immediately"
// - Past timestamps return (0, true) meaning "retry immediately"
// - Missing or invalid headers return (0, false)
//
// Returns (0, false) if no valid backoff duration can be determined from headers.
func Get429BackoffTime(headers http.Header) (time.Duration, bool) {
	if headers == nil {
		return 0, false
	}

	// 1. Check for standard Retry-After header (RFC 6585)
	retryAfter := headers.Get("Retry-After")
	if retryAfter != "" {
		// Try to parse as seconds (integer)
		// Accept 0 or negative as valid (means retry immediately)
		if seconds, err := time.ParseDuration(retryAfter + "s"); err == nil {
			if seconds <= 0 {
				return 0, true // Normalize to 0 for immediate retry
			}
			return seconds, true
		}

		// Try to parse as HTTP date
		if retryTime, err := http.ParseTime(retryAfter); err == nil {
			duration := time.Until(retryTime)
			if duration <= 0 {
				return 0, true // Past timestamp means retry immediately
			}
			return duration, true
		}
	}

	// 2. Check for X-Rate-Limit-Reset family headers (Unix timestamp)
	// Try multiple header name variations used by different APIs
	var resetHeader string
	for _, headerName := range []string{"X-Rate-Limit-Reset", "X-RateLimit-Reset", "RateLimit-Reset"} {
		if val := headers.Get(headerName); val != "" {
			resetHeader = val
			break
		}
	}

	if resetHeader != "" {
		// Parse Unix timestamp
		resetTimestamp, err := strconv.ParseInt(resetHeader, 10, 64)
		if err == nil {
			// Use Date header if present for clock-skew compensation (Okta style)
			dateHeader := headers.Get("Date")
			if dateHeader != "" {
				if requestDate, err := http.ParseTime(dateHeader); err == nil {
					// Calculate backoff using server's time reference
					backoffSeconds := resetTimestamp - requestDate.Unix() + 1
					if backoffSeconds <= 0 {
						return 0, true // Past timestamp means retry immediately
					}
					return time.Duration(backoffSeconds) * time.Second, true
				}
			}

			// Fallback: calculate directly from current time
			resetTime := time.Unix(resetTimestamp, 0)
			duration := time.Until(resetTime)
			if duration <= 0 {
				return 0, true // Past timestamp means retry immediately
			}
			return duration, true
		}
	}

	return 0, false
}
