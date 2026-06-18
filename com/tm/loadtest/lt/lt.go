// Package lt provides shared helpers for booking-system load tests:
// a lightweight Prometheus text-format counter registry, a tuned HTTP client,
// opportunity creation, and claim-sending primitives used by all scenarios.
//
// Driver IDs are globally unique within a process run (atomic counter), so
// tests running concurrently never accidentally share a driver across opps.
package lt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ── Global sequence (unique across all goroutines / scenarios) ────────────────

var seq atomic.Int64

// newSeq reserves n sequential IDs and returns the first one.
func newSeq(n int) int64 {
	end := seq.Add(int64(n))
	return end - int64(n)
}

// ── Prometheus text-format counter registry ───────────────────────────────────

// Reg is a minimal, thread-safe Prometheus counter registry with two fixed
// label dimensions: scenario and result.
type Reg struct {
	name string
	help string
	mu   sync.Mutex
	vals map[[2]string]*atomic.Int64
}

// NewReg creates a registry for the given metric name.
func NewReg(name, help string) *Reg {
	return &Reg{name: name, help: help, vals: make(map[[2]string]*atomic.Int64)}
}

// Inc increments the (scenario, result) counter by 1.
func (r *Reg) Inc(scenario, result string) {
	key := [2]string{scenario, result}
	r.mu.Lock()
	c, ok := r.vals[key]
	if !ok {
		c = &atomic.Int64{}
		r.vals[key] = c
	}
	r.mu.Unlock()
	c.Add(1)
}

// Text returns Prometheus exposition format for all counters.
func (r *Reg) Text() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "# HELP %s %s\n# TYPE %s counter\n", r.name, r.help, r.name)
	r.mu.Lock()
	snapshot := make(map[[2]string]int64, len(r.vals))
	for k, v := range r.vals {
		snapshot[k] = v.Load()
	}
	r.mu.Unlock()
	for k, v := range snapshot {
		fmt.Fprintf(&sb, "%s{scenario=%q,result=%q} %d\n", r.name, k[0], k[1], v)
	}
	return sb.String()
}

// ── HTTP client ───────────────────────────────────────────────────────────────

// NewClient returns an http.Client tuned for high-concurrency load tests.
func NewClient(maxConns int) *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        maxConns + 100,
			MaxIdleConnsPerHost: maxConns + 100,
			MaxConnsPerHost:     maxConns + 100,
			IdleConnTimeout:     60 * time.Second,
		},
	}
}

// ── Opportunity creation ──────────────────────────────────────────────────────

// CreateOpp calls POST /opportunities/:id.
// windowStart and windowEnd are Unix epoch seconds (as expected by the server).
func CreateOpp(client *http.Client, target, oppID, region, zone string, capacity int, windowStart, windowEnd int64) error {
	body, _ := json.Marshal(map[string]any{
		"region_id":            region,
		"zone_id":              zone,
		"capacity":             capacity,
		"booking_window_start": windowStart,
		"booking_window_end":   windowEnd,
	})
	req, err := http.NewRequest(http.MethodPost, target+"/opportunities/"+oppID, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create opp %s: got %d", oppID, resp.StatusCode)
	}
	return nil
}

// CreateOpps creates n opportunities in parallel (concurrency=50).
// IDs are "<prefix>-NNNN-<timestamp>"; windowStart/End are Unix seconds.
func CreateOpps(client *http.Client, target, prefix, region, zone string, n, capacity int, windowStart, windowEnd int64) ([]*OppPool, error) {
	pools := make([]*OppPool, n)
	ts := time.Now().UnixMilli()
	for i := range pools {
		pools[i] = &OppPool{ID: fmt.Sprintf("%s-%04d-%d", prefix, i, ts)}
	}

	sem := make(chan struct{}, 50)
	errc := make(chan error, n)
	var wg sync.WaitGroup
	for i, p := range pools {
		wg.Add(1)
		sem <- struct{}{}
		go func(id string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := CreateOpp(client, target, id, region, zone, capacity, windowStart, windowEnd); err != nil {
				errc <- err
			}
		}(p.ID)
		_ = i
	}
	wg.Wait()
	close(errc)
	if err := <-errc; err != nil {
		return nil, err
	}
	return pools, nil
}

// ── Claim sending ─────────────────────────────────────────────────────────────

// Req is a single claim request.
type Req struct {
	OppID    string
	DriverID string
	IdemKey  string
	// CallerID rides the X-Caller-Id header → ClaimEvent → manager, so every
	// server/manager/PG metric for this request is tagged by the scenario that
	// issued it (e.g. "contended" / "diverse" / "realistic"). Empty = header omitted.
	CallerID string
}

// Label maps an HTTP response to a Prometheus result label.
// It reads the body to distinguish FULL from CLOSED (both are 409).
func Label(status int, body []byte) string {
	switch status {
	case 202:
		return "accepted"
	case 200:
		return "dup"
	case 409:
		if bytes.Contains(body, []byte("CLOSED")) {
			return "closed"
		}
		return "full"
	case 503:
		// Server returns 503 for two distinct reasons, distinguished by body:
		//   "THROTTLED"   = Redis circuit breaker open (degraded throttle)
		//   "UNAVAILABLE" = real server-side failure (Kafka publish / gate error)
		if bytes.Contains(body, []byte("THROTTLED")) {
			return "throttled"
		}
		return "error"
	default:
		return "error"
	}
}

// SendClaim fires POST /opportunities/:id/bookings and returns (status, body).
func SendClaim(client *http.Client, target string, r Req) (int, []byte) {
	req, _ := http.NewRequest(http.MethodPost, target+"/opportunities/"+r.OppID+"/bookings", nil)
	req.Header.Set("X-Driver-Id", r.DriverID)
	req.Header.Set("X-Idempotency-Key", r.IdemKey)
	if r.CallerID != "" {
		req.Header.Set("X-Caller-Id", r.CallerID)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, body
}

// FireAtRPS sends reqs at the given RPS, recording outcomes in reg[scenario].
// Each request is dispatched in a goroutine; returns when all complete.
func FireAtRPS(client *http.Client, target string, reqs []Req, rps int, reg *Reg, scenario string) {
	if len(reqs) == 0 || rps <= 0 {
		return
	}
	tick := time.NewTicker(time.Second / time.Duration(rps))
	defer tick.Stop()
	var wg sync.WaitGroup
	for _, r := range reqs {
		<-tick.C
		wg.Add(1)
		r := r
		go func() {
			defer wg.Done()
			status, body := SendClaim(client, target, r)
			reg.Inc(scenario, Label(status, body))
		}()
	}
	wg.Wait()
}

// FireAll sends all reqs concurrently (no rate limiting).
func FireAll(client *http.Client, target string, reqs []Req, reg *Reg, scenario string) {
	var wg sync.WaitGroup
	for _, r := range reqs {
		wg.Add(1)
		r := r
		go func() {
			defer wg.Done()
			status, body := SendClaim(client, target, r)
			reg.Inc(scenario, Label(status, body))
		}()
	}
	wg.Wait()
}

// ── Request list building ─────────────────────────────────────────────────────

// BuildReqs builds a flat request list for one opp:
//   - uniqueN requests with globally unique driver IDs
//   - dupN re-sends of the first min(dupN, uniqueN) drivers in this batch
//
// Dup requests reuse the same driverID+idemKey as the original, so the gate
// returns DUP (200) once the driver is in the claimed_set.
func BuildReqs(oppID string, uniqueN, dupN int, caller string) []Req {
	start := newSeq(uniqueN)
	reqs := make([]Req, 0, uniqueN+dupN)

	for i := 0; i < uniqueN; i++ {
		id := fmt.Sprintf("d%d", start+int64(i))
		reqs = append(reqs, Req{OppID: oppID, DriverID: id, IdemKey: id + "k", CallerID: caller})
	}

	// Dup targets: first min(dupN, uniqueN) unique requests from this batch.
	// Sent last so originals are in claimed_set before dups arrive.
	poolSize := uniqueN
	if poolSize > dupN {
		poolSize = dupN
	}
	for i := 0; i < dupN; i++ {
		orig := reqs[i%poolSize]
		reqs = append(reqs, Req{OppID: oppID, DriverID: orig.DriverID, IdemKey: orig.IdemKey, CallerID: caller})
	}
	return reqs
}

// NewDriver returns a globally unique (driver_id, idempotency_key) pair safe to
// use across concurrent goroutines and multiple test runs within one process.
func NewDriver() (id, iemKey string) {
	n := seq.Add(1)
	id = fmt.Sprintf("d%d", n)
	return id, id + "k"
}

// ── Per-opp state for pg load tests ──────────────────────────────────────────

// sentDriver holds a driver that was previously sent to an opp (for dup selection).
type sentDriver struct {
	id      string
	iemKey  string
}

// OppPool tracks per-opp sent drivers so each tick can generate dup re-sends.
type OppPool struct {
	ID  string
	mu  sync.Mutex
	buf []sentDriver // bounded ring (newest at tail)
}

const maxBuf = 500

// NextReqs generates count requests for this opp:
// floor(count*(1-dupFrac)) unique + remainder dup re-sends of past drivers.
func (p *OppPool) NextReqs(count int, dupFrac float64, caller string) []Req {
	dupN := int(float64(count)*dupFrac + 0.5)
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.buf) == 0 {
		dupN = 0 // no history yet on first tick
	}
	uniqueN := count - dupN

	start := newSeq(uniqueN)
	reqs := make([]Req, 0, count)

	for i := 0; i < uniqueN; i++ {
		id := fmt.Sprintf("d%d", start+int64(i))
		key := id + "k"
		p.buf = append(p.buf, sentDriver{id, key})
		reqs = append(reqs, Req{OppID: p.ID, DriverID: id, IdemKey: key, CallerID: caller})
	}
	if len(p.buf) > maxBuf {
		p.buf = p.buf[len(p.buf)-maxBuf:]
	}
	for i := 0; i < dupN; i++ {
		d := p.buf[rand.Intn(len(p.buf))]
		reqs = append(reqs, Req{OppID: p.ID, DriverID: d.id, IdemKey: d.iemKey, CallerID: caller})
	}
	return reqs
}

// NextReq generates 1 request: with dupProb probability it re-sends a previously
// accepted driver (DUP), otherwise a fresh unique driver.
func (p *OppPool) NextReq(dupProb float64, caller string) Req {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.buf) > 0 && rand.Float64() < dupProb {
		d := p.buf[rand.Intn(len(p.buf))]
		return Req{OppID: p.ID, DriverID: d.id, IdemKey: d.iemKey, CallerID: caller}
	}
	n := newSeq(1)
	id := fmt.Sprintf("d%d", n)
	key := id + "k"
	p.buf = append(p.buf, sentDriver{id, key})
	if len(p.buf) > maxBuf {
		p.buf = p.buf[1:]
	}
	return Req{OppID: p.ID, DriverID: id, IdemKey: key, CallerID: caller}
}
