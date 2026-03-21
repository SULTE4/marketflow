package domain

const (
	StatusOK = "ok"

	StatusDown = "down"

	StatusDegraded = "degraded"
)

type ComponentHealth struct {
	Status string `json:"status"`

	Message string `json:"message,omitempty"`
}

type HealthResult struct {
	Status string `json:"status"`

	Postgres ComponentHealth `json:"postgres"`

	Redis ComponentHealth `json:"redis"`
}

func (h *HealthResult) IsHealthy() bool {
	return h.Status == StatusOK
}
