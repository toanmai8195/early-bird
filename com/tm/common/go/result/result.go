// Package result holds the shared claim-outcome types used by the Go client
// (and any future Go tooling) to interpret server responses.
package result

// Outcome mirrors the server's classification of a claim attempt.
type Outcome string

const (
	Accepted Outcome = "ACCEPTED" // 202/200 — slot acquired or idempotent retry
	Full     Outcome = "FULL"     // 409 — capacity reached, fast-rejected
	Unavail  Outcome = "UNAVAILABLE"
)

// FromStatus maps an HTTP status code to an Outcome.
func FromStatus(code int) Outcome {
	switch code {
	case 200, 202:
		return Accepted
	case 409:
		return Full
	default:
		return Unavail
	}
}

// Tally accumulates outcomes across a load-test run.
type Tally struct {
	Accepted int
	Full     int
	Unavail  int
}

// Add records one outcome.
func (t *Tally) Add(o Outcome) {
	switch o {
	case Accepted:
		t.Accepted++
	case Full:
		t.Full++
	default:
		t.Unavail++
	}
}
