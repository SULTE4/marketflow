package domain

// Status constants used by both ComponentHealth and HealthResult.
const (
	// StatusOK means the component is reachable and responding normally.
	StatusOK = "ok"

	// StatusDown means the component is unreachable or returned an error.
	StatusDown = "down"

	// StatusDegraded means at least one component is down but not all of them.
	StatusDegraded = "degraded"
)

// ComponentHealth carries the health status of a single infrastructure
// component (database, cache, etc.).
type ComponentHealth struct {
	// Status is one of StatusOK or StatusDown.
	Status string `json:"status"`

	// Message is populated only when Status is StatusDown.  It contains the
	// raw error string returned by the component's Ping call, which gives
	// operators actionable diagnostic information (e.g. "connection refused",
	// "i/o timeout").
	Message string `json:"message,omitempty"`
}

// HealthResult is the structured response returned by the health endpoint.
// Both components are always checked and reported independently, so the
// caller always receives the full picture even when one service is down.
type HealthResult struct {
	// Status is the overall health of the application:
	//   "ok"       – all components healthy
	//   "degraded" – one or more components down, but not all
	//   "down"     – all components down
	Status string `json:"status"`

	// Postgres reports the health of the PostgreSQL database.
	Postgres ComponentHealth `json:"postgres"`

	// Redis reports the health of the Redis cache.
	Redis ComponentHealth `json:"redis"`
}

// IsHealthy returns true only when every component is reporting StatusOK.
func (h *HealthResult) IsHealthy() bool {
	return h.Status == StatusOK
}
