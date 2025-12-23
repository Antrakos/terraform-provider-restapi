package restapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"golang.org/x/time/rate"
)

// Package-level PRNG for jitter calculation
var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

// RetryExhaustedError wraps the final error after exhausting retries,
// preserving HTTP status code and retry context
type RetryExhaustedError struct {
	LastError  error
	RetryCount int
	StatusCode int
	Body       string
}

func (e *RetryExhaustedError) Error() string {
	return fmt.Sprintf("%v (exhausted %d retries)", e.LastError, e.RetryCount)
}

func (e *RetryExhaustedError) Unwrap() error {
	return e.LastError
}

type apiClientOpt struct {
	uri                 string
	insecure            bool
	username            string
	password            string
	headers             map[string]string
	timeout             int
	idAttribute         string
	createMethod        string
	readMethod          string
	readData            string
	updateMethod        string
	updateData          string
	destroyMethod       string
	destroyData         string
	copyKeys            []string
	writeReturnsObject  bool
	createReturnsObject bool
	xssiPrefix          string
	useCookies          bool
	rateLimit           float64
	maxRetries          int
	minBackoff          float64
	maxBackoff          float64
	oauthClientID       string
	oauthClientSecret   string
	oauthScopes         []string
	oauthTokenURL       string
	oauthEndpointParams url.Values
	certFile            string
	keyFile             string
	rootCAFile          string
	certString          string
	keyString           string
	rootCAString        string
	debug               bool
}

/*APIClient is a HTTP client with additional controlling fields*/
type APIClient struct {
	httpClient          *http.Client
	uri                 string
	insecure            bool
	username            string
	password            string
	headers             map[string]string
	idAttribute         string
	createMethod        string
	readMethod          string
	readData            string
	updateMethod        string
	updateData          string
	destroyMethod       string
	destroyData         string
	copyKeys            []string
	writeReturnsObject  bool
	createReturnsObject bool
	xssiPrefix          string
	rateLimiter         *rate.Limiter
	maxRetries          int
	minBackoff          float64
	maxBackoff          float64
	debug               bool
	oauthConfig         *clientcredentials.Config
}

// NewAPIClient makes a new api client for RESTful calls
func NewAPIClient(opt *apiClientOpt) (*APIClient, error) {
	if opt.debug {
		log.Printf("api_client.go: Constructing debug api_client\n")
	}

	if opt.uri == "" {
		return nil, errors.New("uri must be set to construct an API client")
	}

	// Validate retry configuration
	if opt.maxRetries > 0 {
		if opt.minBackoff <= 0 {
			return nil, fmt.Errorf("min_backoff must be positive when retries are enabled, got %f", opt.minBackoff)
		}
		if opt.maxBackoff <= 0 {
			return nil, fmt.Errorf("max_backoff must be positive when retries are enabled, got %f", opt.maxBackoff)
		}
		if opt.minBackoff > opt.maxBackoff {
			return nil, fmt.Errorf("min_backoff (%f) must be <= max_backoff (%f)", opt.minBackoff, opt.maxBackoff)
		}
	}

	/* Sane default */
	if opt.idAttribute == "" {
		opt.idAttribute = "id"
	}

	/* Remove any trailing slashes since we will append
	   to this URL with our own root-prefixed location */
	opt.uri = strings.TrimSuffix(opt.uri, "/")

	if opt.createMethod == "" {
		opt.createMethod = "POST"
	}
	if opt.readMethod == "" {
		opt.readMethod = "GET"
	}
	if opt.updateMethod == "" {
		opt.updateMethod = "PUT"
	}
	if opt.destroyMethod == "" {
		opt.destroyMethod = "DELETE"
	}

	tlsConfig := &tls.Config{
		/* Disable TLS verification if requested */
		InsecureSkipVerify: opt.insecure,
	}

	if opt.certString != "" && opt.keyString != "" {
		cert, err := tls.X509KeyPair([]byte(opt.certString), []byte(opt.keyString))
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if opt.certFile != "" && opt.keyFile != "" {
		cert, err := tls.LoadX509KeyPair(opt.certFile, opt.keyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load root CA
	if opt.rootCAFile != "" || opt.rootCAString != "" {
		caCertPool := x509.NewCertPool()
		var rootCA []byte
		var err error

		if opt.rootCAFile != "" {
			if opt.debug {
				log.Printf("api_client.go: Reading root CA file: %s\n", opt.rootCAFile)
			}
			rootCA, err = os.ReadFile(opt.rootCAFile)
			if err != nil {
				return nil, fmt.Errorf("could not read root CA file: %v", err)
			}
		} else {
			if opt.debug {
				log.Printf("api_client.go: Using provided root CA string\n")
			}
			rootCA = []byte(opt.rootCAString)
		}

		if !caCertPool.AppendCertsFromPEM(rootCA) {
			return nil, errors.New("failed to append root CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	tr := &http.Transport{
		TLSClientConfig: tlsConfig,
		Proxy:           http.ProxyFromEnvironment,
	}

	var cookieJar http.CookieJar

	if opt.useCookies {
		cookieJar, _ = cookiejar.New(nil)
	}

	rateLimit := rate.Limit(opt.rateLimit)
	bucketSize := int(math.Max(math.Round(opt.rateLimit), 1))
	log.Printf("limit: %f bucket: %d", opt.rateLimit, bucketSize)
	rateLimiter := rate.NewLimiter(rateLimit, bucketSize)

	client := APIClient{
		httpClient: &http.Client{
			Timeout:   time.Second * time.Duration(opt.timeout),
			Transport: tr,
			Jar:       cookieJar,
		},
		rateLimiter:         rateLimiter,
		uri:                 opt.uri,
		insecure:            opt.insecure,
		username:            opt.username,
		password:            opt.password,
		headers:             opt.headers,
		idAttribute:         opt.idAttribute,
		createMethod:        opt.createMethod,
		readMethod:          opt.readMethod,
		readData:            opt.readData,
		updateMethod:        opt.updateMethod,
		updateData:          opt.updateData,
		destroyMethod:       opt.destroyMethod,
		destroyData:         opt.destroyData,
		copyKeys:            opt.copyKeys,
		writeReturnsObject:  opt.writeReturnsObject,
		createReturnsObject: opt.createReturnsObject,
		xssiPrefix:          opt.xssiPrefix,
		maxRetries:          opt.maxRetries,
		minBackoff:          opt.minBackoff,
		maxBackoff:          opt.maxBackoff,
		debug:               opt.debug,
	}

	if opt.oauthClientID != "" && opt.oauthClientSecret != "" && opt.oauthTokenURL != "" {
		client.oauthConfig = &clientcredentials.Config{
			ClientID:       opt.oauthClientID,
			ClientSecret:   opt.oauthClientSecret,
			TokenURL:       opt.oauthTokenURL,
			Scopes:         opt.oauthScopes,
			EndpointParams: opt.oauthEndpointParams,
		}
	}

	if opt.debug {
		log.Printf("api_client.go: Constructed client:\n%s", client.toString())
	}
	return &client, nil
}

// Convert the important bits about this object to string representation
// This is useful for debugging.
func (client *APIClient) toString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("uri: %s\n", client.uri))
	buffer.WriteString(fmt.Sprintf("insecure: %t\n", client.insecure))
	buffer.WriteString(fmt.Sprintf("username: %s\n", client.username))
	buffer.WriteString(fmt.Sprintf("password: %s\n", client.password))
	buffer.WriteString(fmt.Sprintf("id_attribute: %s\n", client.idAttribute))
	buffer.WriteString(fmt.Sprintf("write_returns_object: %t\n", client.writeReturnsObject))
	buffer.WriteString(fmt.Sprintf("create_returns_object: %t\n", client.createReturnsObject))
	buffer.WriteString("headers:\n")
	for k, v := range client.headers {
		buffer.WriteString(fmt.Sprintf("  %s: %s\n", k, v))
	}
	for _, n := range client.copyKeys {
		buffer.WriteString(fmt.Sprintf("  %s", n))
	}
	return buffer.String()
}

// httpResponse carries response data including headers for retry decisions
type httpResponse struct {
	statusCode int
	body       string
	headers    http.Header
}

// restapiBackoff implements the backoff.BackOff interface for retry logic
type restapiBackoff struct {
	retryCount, maxRetries int
	backoffDuration        time.Duration
}

// NextBackOff returns the duration to wait before retrying the operation,
// or backoff.Stop to indicate that no more retries should be made.
func (r *restapiBackoff) NextBackOff() time.Duration {
	if r.retryCount > r.maxRetries {
		return backoff.Stop
	}
	return r.backoffDuration
}

// Reset to initial state.
func (r *restapiBackoff) Reset() {}

// isIdempotent returns true if the HTTP method is safe to retry
func isIdempotent(method string) bool {
	// Safe methods per RFC 7231
	switch method {
	case "GET", "HEAD", "OPTIONS", "PUT", "DELETE":
		return true
	default:
		return false
	}
}

/*
Helper function that handles sending/receiving and handling

	of HTTP data in and out.
*/
func (client *APIClient) sendRequest(method string, path string, data string) (string, error) {
	fullURI := client.uri + path

	if client.debug {
		log.Printf("api_client.go: method='%s', path='%s', full uri (derived)='%s', data='%s'\n", method, path, fullURI, data)
	}

	// If retries are disabled, use the original non-retry logic
	if client.maxRetries <= 0 {
		resp, err := client.doRequest(method, fullURI, data)
		if resp != nil {
			return resp.body, err
		}
		return "", err
	}

	// Retry logic enabled
	var lastBody string
	var lastErr error
	var lastStatusCode int

	bOff := &restapiBackoff{
		maxRetries: client.maxRetries,
	}

	operation := func() error {
		resp, err := client.doRequest(method, fullURI, data)
		if resp != nil {
			lastStatusCode = resp.statusCode
		}
		lastErr = err

		if err != nil {
			// Check if this method is safe to retry
			if !isIdempotent(method) {
				if client.debug {
					log.Printf("api_client.go: Not retrying non-idempotent %s request\n", method)
				}
				return backoff.Permanent(err)
			}

			// Check if this is a retryable HTTP status code
			if resp != nil && resp.statusCode == http.StatusTooManyRequests {
				// Rate limit error - extract backoff time from Retry-After header
				backoffDuration, found := Get429BackoffTime(resp.headers)
				if !found {
					// No valid header found, use min backoff
					backoffDuration = time.Duration(client.minBackoff * float64(time.Second))
				}
				maxBackoffDuration := time.Duration(client.maxBackoff * float64(time.Second))
				if backoffDuration > maxBackoffDuration {
					backoffDuration = maxBackoffDuration
				}
				bOff.backoffDuration = backoffDuration
				bOff.retryCount++

				if client.debug {
					log.Printf("api_client.go: Rate limited (429), retry %d/%d after %v\n", bOff.retryCount, client.maxRetries, backoffDuration)
				}
				return fmt.Errorf("rate limited, will retry")
			}

			if resp != nil && resp.statusCode == http.StatusServiceUnavailable {
				// Service unavailable - use exponential backoff with jitter
				backoffSeconds := math.Min(math.Pow(2, float64(bOff.retryCount))*client.minBackoff, client.maxBackoff)
				// Add ±25% jitter to prevent thundering herd (75-125% of calculated backoff)
				jitter := 0.75 + rng.Float64()*0.5
				backoffSeconds = backoffSeconds * jitter
				bOff.backoffDuration = time.Duration(backoffSeconds * float64(time.Second))
				bOff.retryCount++
				if client.debug {
					log.Printf("api_client.go: Service unavailable (503), retry %d/%d after %v\n", bOff.retryCount, client.maxRetries, bOff.backoffDuration)
				}
				return fmt.Errorf("service unavailable, will retry")
			}

			var netErr net.Error
			var urlErr *url.Error
			if errors.Is(err, io.EOF) || errors.As(err, &netErr) || errors.As(err, &urlErr) {
				backoffSeconds := math.Min(math.Pow(2, float64(bOff.retryCount))*client.minBackoff, client.maxBackoff)
				// Add ±25% jitter to prevent thundering herd (75-125% of calculated backoff)
				jitter := 0.75 + rng.Float64()*0.5
				backoffSeconds = backoffSeconds * jitter
				bOff.backoffDuration = time.Duration(backoffSeconds * float64(time.Second))
				bOff.retryCount++

				if client.debug {
					log.Printf("api_client.go: Network error, retry %d/%d after %v: %v\n", bOff.retryCount, client.maxRetries, bOff.backoffDuration, err)
				}
				return fmt.Errorf("network error, will retry: %w", err)
			}

			// Non-retryable error
			return backoff.Permanent(err)
		}

		// Success
		if resp != nil {
			lastBody = resp.body
		}
		return nil
	}

	err := backoff.Retry(operation, bOff)
	if err != nil {
		// Return custom error that preserves status code and retry context
		if lastErr != nil {
			return lastBody, &RetryExhaustedError{
				LastError:  lastErr,
				RetryCount: bOff.retryCount,
				StatusCode: lastStatusCode,
				Body:       lastBody,
			}
		}
		return lastBody, err
	}

	return lastBody, nil
}

// doRequest performs a single HTTP request and returns response with headers
func (client *APIClient) doRequest(method string, fullURI string, data string) (*httpResponse, error) {
	var req *http.Request
	var err error

	buffer := bytes.NewBuffer([]byte(data))

	if data == "" {
		req, err = http.NewRequest(method, fullURI, nil)
	} else {
		req, err = http.NewRequest(method, fullURI, buffer)

		/* Default of application/json, but allow headers array to overwrite later */
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
		}
	}

	if err != nil {
		return nil, err
	}

	if client.debug {
		log.Printf("api_client.go: Sending HTTP request to %s...\n", req.URL)
	}

	/* Allow for tokens or other pre-created secrets */
	if len(client.headers) > 0 {
		for n, v := range client.headers {
			req.Header.Set(n, v)
		}
	}

	if client.oauthConfig != nil {
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client.httpClient)
		tokenSource := client.oauthConfig.TokenSource(ctx)
		token, err := tokenSource.Token()
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	}

	if client.username != "" && client.password != "" {
		/* ... and fall back to basic auth if configured */
		req.SetBasicAuth(client.username, client.password)
	}

	if client.debug {
		log.Printf("api_client.go: Request headers:\n")
		for name, headers := range req.Header {
			for _, h := range headers {
				log.Printf("api_client.go:   %v: %v", name, h)
			}
		}

		log.Printf("api_client.go: BODY:\n")
		body := "<none>"
		if req.Body != nil {
			body = string(data)
		}
		log.Printf("%s\n", body)
	}

	if client.rateLimiter != nil {
		// Rate limiting
		if client.debug {
			log.Printf("Waiting for rate limit availability\n")
		}
		_ = client.rateLimiter.Wait(context.Background())
	}

	resp, err := client.httpClient.Do(req)

	if err != nil {
		return nil, err
	}

	if client.debug {
		log.Printf("api_client.go: Response code: %d\n", resp.StatusCode)
		log.Printf("api_client.go: Response headers:\n")
		for name, headers := range resp.Header {
			for _, h := range headers {
				log.Printf("api_client.go:   %v: %v", name, h)
			}
		}
	}

	bodyBytes, err2 := io.ReadAll(resp.Body)
	resp.Body.Close()

	if err2 != nil {
		return nil, err2
	}
	body := strings.TrimPrefix(string(bodyBytes), client.xssiPrefix)
	if client.debug {
		log.Printf("api_client.go: BODY:\n%s\n", body)
	}

	httpResp := &httpResponse{
		statusCode: resp.StatusCode,
		body:       body,
		headers:    resp.Header,
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httpResp, fmt.Errorf("unexpected response code '%d': %s", resp.StatusCode, body)
	}

	if body == "" {
		httpResp.body = "{}"
	}

	return httpResp, nil
}
