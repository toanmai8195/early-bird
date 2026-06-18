// Command pg measures throughput and correctness of the PgClaimStore
// bulk-settle CTE in isolation — no HTTP server, no Redis gate, no Kafka.
//
// ── throughput ───────────────────────────────────────────────────────────────
// Two concurrent scenarios run indefinitely:
//
//   - contended: N opps × B drivers per CTE tick — mirrors manager bulk-settle
//     under hot-partition load (all claims for one opp in one statement).
//   - diverse:   M opps × 1 driver per tick — low-contention across many rows.
//
// ── oversell ─────────────────────────────────────────────────────────────────
// Creates a fresh opp with capacity=C then fires C+extra goroutines (each a
// unique driver) all at once, in one CTE per goroutine.  After all settle:
//
//  1. remaining >= 0  (no negative balance)
//  2. count(bookings) <= capacity  (no overselling)
//  3. remaining + count(bookings) == capacity  (atomic decrement is exact)
//
// Any violation increments loadtest_pg_oversell_violations_total.
// Repeats every --oversell-interval until the process exits.
//
// ── idempotent ───────────────────────────────────────────────────────────────
// Creates a fresh opp, settles a fixed batch of drivers --idempotent-rounds
// times (simulates Kafka at-least-once re-delivery).  Asserts:
//
//  1. count(bookings) == len(batch)   (each driver exactly once)
//  2. remaining == capacity - len(batch)
//
// Violations: loadtest_pg_idempotent_violations_total.
//
// ── ramp ─────────────────────────────────────────────────────────────────────
// Gradually increases concurrent workers from --ramp-start to --ramp-max,
// multiplying by --ramp-factor each step.  Each step creates a fresh opp with
// large capacity and fires that many goroutines (each 1 CTE), then reports
// p50/p99/max latency and throughput.  Useful for finding the saturation point
// of the PG CTE under FOR UPDATE contention.
//
// ── all ──────────────────────────────────────────────────────────────────────
// Runs oversell + idempotent assertions once each, exits non-zero on any
// violation, then starts throughput loop.
//
// Usage:
//
//	bazel run //com/tm/loadtest/pg -- \
//	  --pg=postgres://earlybird:earlybird@localhost:5432/earlybird?sslmode=disable \
//	  --run=all
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"

	"github.com/tm/loadtest/lt"
)

// ── Flags ────────────────────────────────────────────────────────────────────

var (
	pgDSN       = flag.String("pg", "postgres://earlybird:earlybird@localhost:5432/earlybird?sslmode=disable", "Postgres DSN")
	metricsAddr = flag.String("metrics-addr", ":9411", "Prometheus /metrics listen address")
	runMode     = flag.String("run", "throughput", "test mode: throughput|oversell|idempotent|ramp|all")

	// throughput flags
	contendedOpps   = flag.Int("contended-opps", 20, "opps for contended scenario")
	contendedPerOpp = flag.Int("contended-per-opp", 50, "drivers per opp per tick (contended)")
	diverseOpps     = flag.Int("diverse-opps", 1_000, "opps for diverse scenario")
	dupPct          = flag.Float64("dup-pct", 5.0, "percent of requests that are dup re-sends (0–100)")
	capacity        = flag.Int("capacity", 10_000_000, "opportunity capacity (throughput mode)")

	// oversell flags
	oversellCapacity = flag.Int("oversell-capacity", 1_000, "capacity for oversell correctness test")
	oversellExtra    = flag.Int("oversell-extra", 500, "extra concurrent claimants beyond capacity")
	oversellInterval = flag.Duration("oversell-interval", 30*time.Second, "interval between oversell rounds")
	oversellRounds   = flag.Int("oversell-rounds", 3, "number of oversell rounds (0 = infinite)")

	// idempotent flags
	idempotentCapacity = flag.Int("idempotent-capacity", 200, "capacity for idempotent test")
	idempotentBatch    = flag.Int("idempotent-batch", 50, "distinct drivers per idempotent round")
	idempotentRounds   = flag.Int("idempotent-rounds", 5, "times to replay the same batch")

	// ramp flags — nested sweep: for each batch size, run all traffic-rate steps.
	rampRates      = flag.String("ramp-rates", "10,15,20,50,80,100,120,150", "comma-separated traffic rates (CTE/s) to sweep per batch size")
	rampStepDur    = flag.Duration("ramp-step-dur", 2*time.Minute, "how long to hold each (batch, rate) step")
	rampCapacity   = flag.Int("ramp-capacity", 10_000_000, "opp capacity per ramp step (large to avoid REJECTED noise)")
	rampCooldown   = flag.Duration("ramp-cooldown", 15*time.Second, "pause between batch sizes so Grafana shows a clear gap")
	rampBatchSizes = flag.String("ramp-batch-sizes", "10,50,100,200,300", "comma-separated batch sizes (drivers/CTE) to sweep; each runs all traffic steps")

	region = flag.String("region", "lt", "region_id")
	zone   = flag.String("zone", "lt", "zone_id")
)

// ── Global state ─────────────────────────────────────────────────────────────

var driverSeq atomic.Int64

func newDriver() (id, iemKey string) {
	n := driverSeq.Add(1)
	id = fmt.Sprintf("dp%d", n)
	return id, id + "k"
}

// ── Metrics ───────────────────────────────────────────────────────────────────

type latHist struct {
	mu      sync.Mutex
	samples []float64
}

func (h *latHist) record(d time.Duration) {
	h.mu.Lock()
	h.samples = append(h.samples, d.Seconds())
	h.mu.Unlock()
}

func (h *latHist) percentile(p float64) float64 {
	h.mu.Lock()
	s := make([]float64, len(h.samples))
	copy(s, h.samples)
	h.mu.Unlock()
	if len(s) == 0 {
		return 0
	}
	sort.Float64s(s)
	idx := int(float64(len(s)-1) * p)
	return s[idx]
}

func (h *latHist) max() float64 {
	h.mu.Lock()
	s := make([]float64, len(h.samples))
	copy(s, h.samples)
	h.mu.Unlock()
	if len(s) == 0 {
		return 0
	}
	sort.Float64s(s)
	return s[len(s)-1]
}

type latReg struct {
	mu    sync.Mutex
	hists map[string]*latHist // scenario → hist
}

func newLatReg() *latReg { return &latReg{hists: map[string]*latHist{}} }

func (r *latReg) record(scenario string, d time.Duration) {
	r.mu.Lock()
	h := r.hists[scenario]
	if h == nil {
		h = &latHist{}
		r.hists[scenario] = h
	}
	r.mu.Unlock()
	h.record(d)
}

func (r *latReg) text() string {
	r.mu.Lock()
	scenarios := make([]string, 0, len(r.hists))
	for k := range r.hists {
		scenarios = append(scenarios, k)
	}
	hists := make(map[string]*latHist, len(r.hists))
	for k, v := range r.hists {
		hists[k] = v
	}
	r.mu.Unlock()
	sort.Strings(scenarios)

	var b strings.Builder
	metrics := []struct {
		name string
		pct  float64
	}{
		{"loadtest_pg_latency_p50_seconds", 0.50},
		{"loadtest_pg_latency_p99_seconds", 0.99},
		{"loadtest_pg_latency_max_seconds", -1},
	}
	for _, m := range metrics {
		fmt.Fprintf(&b, "# HELP %s PG settle CTE latency\n# TYPE %s gauge\n", m.name, m.name)
		for _, sc := range scenarios {
			h := hists[sc]
			var v float64
			if m.pct < 0 {
				v = h.max()
			} else {
				v = h.percentile(m.pct)
			}
			fmt.Fprintf(&b, "%s{scenario=%q} %g\n", m.name, sc, v)
		}
	}
	return b.String()
}

// oversellViolations / idempotentViolations are counters exposed in /metrics.
var (
	oversellViolations   atomic.Int64
	idempotentViolations atomic.Int64
)

// cteReg counts CTE executions per scenario (1 increment per settleMulti call).
type cteReg struct {
	mu   sync.Mutex
	vals map[string]*atomic.Int64
}

func newCTEReg() *cteReg { return &cteReg{vals: map[string]*atomic.Int64{}} }

func (c *cteReg) inc(scenario string) {
	c.mu.Lock()
	v, ok := c.vals[scenario]
	if !ok {
		v = &atomic.Int64{}
		c.vals[scenario] = v
	}
	c.mu.Unlock()
	v.Add(1)
}

func (c *cteReg) text() string {
	c.mu.Lock()
	snap := make(map[string]int64, len(c.vals))
	for k, v := range c.vals {
		snap[k] = v.Load()
	}
	c.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "# HELP loadtest_pg_cte_total PG settle CTE executions by scenario\n# TYPE loadtest_pg_cte_total counter\n")
	for sc, v := range snap {
		fmt.Fprintf(&b, "loadtest_pg_cte_total{scenario=%q} %d\n", sc, v)
	}
	return b.String()
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	db, err := sql.Open("postgres", *pgDSN)
	if err != nil {
		log.Fatalf("open PG: %v", err)
	}
	defer db.Close()
	maxConns := min(*contendedOpps+*diverseOpps+100, 250)
	db.SetMaxOpenConns(maxConns)
	// Fully-warm pool, mirroring the manager's HikariCP config (PgPool.java sets
	// minIdle == max): keep every connection open so connect-latency spikes don't
	// show up mid-load. MaxIdle == MaxOpen avoids close/reopen churn when >half the
	// pool is in use; ConnMaxLifetime=0 keeps idle connections from expiring.
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		log.Fatalf("ping PG: %v", err)
	}
	log.Printf("PG connected (%s)", *pgDSN)

	reg := lt.NewReg(
		"loadtest_pg_claims_total",
		"PG bulk-settle CTE outcomes by scenario and result",
	)
	lat := newLatReg()
	cte := newCTEReg()
	rr := newRampReg()

	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, reg.Text())
		fmt.Fprint(w, cte.text())
		fmt.Fprint(w, lat.text())
		fmt.Fprint(w, rr.text())
		fmt.Fprintf(w,
			"# HELP loadtest_pg_oversell_violations_total PG oversell correctness violations\n"+
				"# TYPE loadtest_pg_oversell_violations_total counter\n"+
				"loadtest_pg_oversell_violations_total %d\n",
			oversellViolations.Load(),
		)
		fmt.Fprintf(w,
			"# HELP loadtest_pg_idempotent_violations_total PG idempotent correctness violations\n"+
				"# TYPE loadtest_pg_idempotent_violations_total counter\n"+
				"loadtest_pg_idempotent_violations_total %d\n",
			idempotentViolations.Load(),
		)
	})
	go func() {
		log.Printf("metrics on %s/metrics", *metricsAddr)
		if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
			log.Fatalf("metrics: %v", err)
		}
	}()

	mode := *runMode
	ok := true

	if mode == "oversell" || mode == "all" {
		ok = runOversell(db, reg, cte, lat) && ok
	}
	if mode == "idempotent" || mode == "all" {
		ok = runIdempotent(db, reg, cte, lat) && ok
	}
	if mode == "ramp" {
		runRamp(db, rr)
	}
	if mode == "throughput" || mode == "all" {
		runThroughput(db, reg, cte, lat)
	}

	if !ok {
		os.Exit(1)
	}
}

// ── Oversell test ─────────────────────────────────────────────────────────────

func runOversell(db *sql.DB, reg *lt.Reg, cte *cteReg, lat *latReg) bool {
	cap := *oversellCapacity
	extra := *oversellExtra
	total := cap + extra
	rounds := *oversellRounds
	interval := *oversellInterval

	log.Printf("[oversell] cap=%d workers=%d rounds=%d", cap, total, rounds)

	allOk := true
	for round := 1; rounds == 0 || round <= rounds; round++ {
		oppID := fmt.Sprintf("lt-ov-%d-%d", round, time.Now().UnixMilli())
		if err := insertOpp(db, oppID, cap); err != nil {
			log.Printf("[oversell] setup: %v", err)
			allOk = false
			break
		}

		// Fire total unique drivers all at once; each driver sends 1 claim in its
		// own CTE call (1 driver per CTE mirrors the worst-case Kafka fanout).
		// database/sql pools connections; goroutines beyond MaxOpenConns queue.
		var wg sync.WaitGroup
		for i := 0; i < total; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				dID, iKey := newDriver()
				start := time.Now()
				settleOne(db, oppID, dID, iKey, reg, cte, "oversell")
				lat.record("oversell", time.Since(start))
			}()
		}
		wg.Wait()

		// Assert correctness.
		var remaining, booked int
		row := db.QueryRow(
			`SELECT o.remaining,
			        (SELECT count(*) FROM bookings b WHERE b.opportunity_id = o.opportunity_id)
			 FROM opportunities o WHERE o.opportunity_id = $1`, oppID)
		if err := row.Scan(&remaining, &booked); err != nil {
			log.Printf("[oversell] r%d query: %v", round, err)
			allOk = false
			oversellViolations.Add(1)
			continue
		}

		pass := remaining >= 0 && booked <= cap && remaining+booked == cap
		if pass {
			log.Printf("[oversell] r%d PASS: remaining=%d booked=%d cap=%d", round, remaining, booked, cap)
		} else {
			log.Printf("[oversell] r%d FAIL: remaining=%d booked=%d cap=%d — OVERSELL", round, remaining, booked, cap)
			oversellViolations.Add(1)
			allOk = false
		}

		if rounds == 0 || round < rounds {
			time.Sleep(interval)
		}
	}
	return allOk
}

// ── Idempotent test ───────────────────────────────────────────────────────────

func runIdempotent(db *sql.DB, reg *lt.Reg, cte *cteReg, lat *latReg) bool {
	cap := *idempotentCapacity
	batchSize := *idempotentBatch
	rounds := *idempotentRounds

	if batchSize > cap {
		log.Printf("[idempotent] batch(%d) > capacity(%d), capping batch", batchSize, cap)
		batchSize = cap
	}

	log.Printf("[idempotent] cap=%d batch=%d replay-rounds=%d", cap, batchSize, rounds)

	oppID := fmt.Sprintf("lt-idem-%d", time.Now().UnixMilli())
	if err := insertOpp(db, oppID, cap); err != nil {
		log.Printf("[idempotent] setup: %v", err)
		return false
	}

	// Fixed batch of drivers replayed N times.
	type pair struct{ id, key string }
	batch := make([]pair, batchSize)
	for i := range batch {
		id, key := newDriver()
		batch[i] = pair{id, key}
	}
	drivers := make([]driverPair, batchSize)
	for i, p := range batch {
		drivers[i] = driverPair{p.id, p.key}
	}

	allOk := true
	for round := 1; round <= rounds; round++ {
		start := time.Now()
		settleMulti(db, oppID, drivers, reg, cte, "idempotent")
		lat.record("idempotent", time.Since(start))
		log.Printf("[idempotent] replay round %d/%d done", round, rounds)
	}

	// Assert.
	var remaining, booked int
	row := db.QueryRow(
		`SELECT o.remaining,
		        (SELECT count(*) FROM bookings b WHERE b.opportunity_id = o.opportunity_id)
		 FROM opportunities o WHERE o.opportunity_id = $1`, oppID)
	if err := row.Scan(&remaining, &booked); err != nil {
		log.Printf("[idempotent] query: %v", err)
		idempotentViolations.Add(1)
		return false
	}

	wantBooked := batchSize
	wantRemaining := cap - batchSize
	pass := booked == wantBooked && remaining == wantRemaining
	if pass {
		log.Printf("[idempotent] PASS: booked=%d remaining=%d (replayed %d×)", booked, remaining, rounds)
	} else {
		log.Printf("[idempotent] FAIL: booked=%d (want %d) remaining=%d (want %d)",
			booked, wantBooked, remaining, wantRemaining)
		idempotentViolations.Add(1)
		allOk = false
	}
	return allOk
}

// ── Ramp test (batch × traffic sweep) ─────────────────────────────────────────
//
// Nested ramp on a single hot opportunity (1 FOR UPDATE row — the contended case
// where batch size is the lever). The OUTER loop sweeps batch size (drivers/CTE)
// through --ramp-batch-sizes (e.g. 10,50,100,200,300); the INNER loop sweeps the
// traffic rate (CTE/s) through --ramp-rates. Every (batch, rate) pair runs for
// --ramp-step-dur (default 2m). A fresh opp per step keeps it a hot single row
// without exhausting capacity over a long run.
//
// Metrics are tagged by `batch` so Grafana shows, as the batch grows: CTE traffic
// (CTE/s), driver traffic (drivers/s = CTE/s × batch), and per-CTE latency. Two
// gauges expose the current step (batch size + target rate / "traffic").

// rampReg collects the batch-tagged ramp metrics plus the current-step gauges.
type rampReg struct {
	mu       sync.Mutex
	cte      map[int]*atomic.Int64      // batch -> CTE executions
	drivers  map[bkResult]*atomic.Int64 // (batch,result) -> driver outcomes
	lat      map[int]*latHist           // batch -> per-CTE latency
	curBatch atomic.Int64
	curRate  atomic.Int64
}

type bkResult struct {
	batch  int
	result string
}

func newRampReg() *rampReg {
	return &rampReg{
		cte:     map[int]*atomic.Int64{},
		drivers: map[bkResult]*atomic.Int64{},
		lat:     map[int]*latHist{},
	}
}

// setStep records the current (batch, rate) so the gauges reflect the live step.
func (r *rampReg) setStep(batch, rate int) {
	r.curBatch.Store(int64(batch))
	r.curRate.Store(int64(rate))
}

func (r *rampReg) incCTE(batch int) {
	r.mu.Lock()
	c, ok := r.cte[batch]
	if !ok {
		c = &atomic.Int64{}
		r.cte[batch] = c
	}
	r.mu.Unlock()
	c.Add(1)
}

func (r *rampReg) incDriver(batch int, result string) {
	k := bkResult{batch, result}
	r.mu.Lock()
	c, ok := r.drivers[k]
	if !ok {
		c = &atomic.Int64{}
		r.drivers[k] = c
	}
	r.mu.Unlock()
	c.Add(1)
}

func (r *rampReg) recordLat(batch int, d time.Duration) {
	r.mu.Lock()
	h, ok := r.lat[batch]
	if !ok {
		h = &latHist{}
		r.lat[batch] = h
	}
	r.mu.Unlock()
	h.record(d)
}

func (r *rampReg) text() string {
	r.mu.Lock()
	cteSnap := make(map[int]int64, len(r.cte))
	for k, v := range r.cte {
		cteSnap[k] = v.Load()
	}
	drvSnap := make(map[bkResult]int64, len(r.drivers))
	for k, v := range r.drivers {
		drvSnap[k] = v.Load()
	}
	latKeys := make([]int, 0, len(r.lat))
	latVals := make(map[int][3]float64, len(r.lat))
	for k, h := range r.lat {
		latKeys = append(latKeys, k)
		latVals[k] = [3]float64{h.percentile(0.50), h.percentile(0.99), h.max()}
	}
	r.mu.Unlock()
	sort.Ints(latKeys)

	var b strings.Builder
	// Current-step gauges: "traffic" (target CTE/s) and batch size.
	fmt.Fprintf(&b, "# HELP loadtest_pg_ramp_rate Current ramp target traffic (CTE/s)\n# TYPE loadtest_pg_ramp_rate gauge\nloadtest_pg_ramp_rate %d\n", r.curRate.Load())
	fmt.Fprintf(&b, "# HELP loadtest_pg_ramp_batch Current ramp batch size (drivers/CTE)\n# TYPE loadtest_pg_ramp_batch gauge\nloadtest_pg_ramp_batch %d\n", r.curBatch.Load())
	// CTE traffic by batch.
	fmt.Fprintf(&b, "# HELP loadtest_pg_ramp_cte_total CTE executions by batch size\n# TYPE loadtest_pg_ramp_cte_total counter\n")
	for batch, v := range cteSnap {
		fmt.Fprintf(&b, "loadtest_pg_ramp_cte_total{batch=\"%d\"} %d\n", batch, v)
	}
	// Driver traffic by batch + outcome.
	fmt.Fprintf(&b, "# HELP loadtest_pg_ramp_claims_total Driver outcomes by batch size and result\n# TYPE loadtest_pg_ramp_claims_total counter\n")
	for k, v := range drvSnap {
		fmt.Fprintf(&b, "loadtest_pg_ramp_claims_total{batch=\"%d\",result=%q} %d\n", k.batch, k.result, v)
	}
	// Per-CTE latency by batch.
	for _, m := range []struct {
		metric string
		idx    int
	}{
		{"loadtest_pg_ramp_latency_p50_seconds", 0},
		{"loadtest_pg_ramp_latency_p99_seconds", 1},
		{"loadtest_pg_ramp_latency_max_seconds", 2},
	} {
		fmt.Fprintf(&b, "# HELP %s Per-CTE settle latency by batch size\n# TYPE %s gauge\n", m.metric, m.metric)
		for _, batch := range latKeys {
			fmt.Fprintf(&b, "%s{batch=\"%d\"} %g\n", m.metric, batch, latVals[batch][m.idx])
		}
	}
	return b.String()
}

func runRamp(db *sql.DB, rr *rampReg) {
	batchSizes := parseIntList("--ramp-batch-sizes", *rampBatchSizes)
	rates := parseIntList("--ramp-rates", *rampRates)
	log.Printf("[ramp] batch sweep %v × rates %v, %s/step",
		batchSizes, rates, *rampStepDur)

	for bi, batch := range batchSizes {
		fmt.Printf("\n=== batch=%d (%d traffic steps × %s) ===\n", batch, len(rates), *rampStepDur)
		fmt.Printf("%-8s %-12s %-12s %-12s %-12s\n", "rate/s", "drivers/s", "p50", "p99", "max")
		for _, rate := range rates {
			rr.setStep(batch, rate)
			// Fresh hot opp per step: 1 opp = 1 FOR UPDATE row (contended); large
			// capacity so a long step never exhausts it (no REJECTED noise).
			oppID := fmt.Sprintf("lt-ramp-c-b%d-r%d-%d", batch, rate, time.Now().UnixMilli())
			if err := insertOpp(db, oppID, *rampCapacity); err != nil {
				log.Printf("[ramp] setup batch=%d rate=%d: %v", batch, rate, err)
				return
			}
			rampStep(db, rr, oppID, batch, rate, *rampStepDur)
		}
		// Cooldown between batch sizes so Grafana shows a clear gap (skip after last).
		if bi < len(batchSizes)-1 {
			log.Printf("[ramp] cooldown %s...", *rampCooldown)
			time.Sleep(*rampCooldown)
		}
	}
	log.Printf("[ramp] done")
}

// rampStep holds one (batch, rate) for dur, dispatching `rate` CTEs/s — each
// settling `batch` fresh unique drivers on the one hot opp — and recording per-CTE
// latency (printed at the end, and fed into the batch-tagged registry live).
func rampStep(db *sql.DB, rr *rampReg, oppID string, batch, rate int, dur time.Duration) {
	h := &latHist{}
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	deadline := time.NewTimer(dur)
	defer deadline.Stop()
	var wg sync.WaitGroup
	for {
		select {
		case <-deadline.C:
			p50 := time.Duration(h.percentile(0.50) * float64(time.Second))
			p99 := time.Duration(h.percentile(0.99) * float64(time.Second))
			mx := time.Duration(h.max() * float64(time.Second))
			wg.Wait()
			fmt.Printf("%-8d %-12d %-12s %-12s %-12s\n", rate, rate*batch,
				p50.Round(time.Millisecond), p99.Round(time.Millisecond), mx.Round(time.Millisecond))
			return
		case <-ticker.C:
			wg.Add(1)
			go func() {
				defer wg.Done()
				drivers := make([]driverPair, batch)
				for i := range drivers {
					id, key := newDriver()
					drivers[i] = driverPair{id, key}
				}
				t0 := time.Now()
				settleRamp(db, oppID, drivers, rr, batch)
				d := time.Since(t0)
				h.record(d)
				rr.recordLat(batch, d)
			}()
		}
	}
}

// settleRamp runs one bulk-settle CTE and records into the ramp registry (CTE count
// + per-driver outcomes, both tagged by batch). Drivers are freshly generated and
// unique, so no in-CTE dedup is needed.
func settleRamp(db *sql.DB, oppID string, drivers []driverPair, rr *rampReg, batch int) {
	if len(drivers) == 0 {
		return
	}
	rr.incCTE(batch)
	query, args := buildSettleSQL(oppID, drivers)
	rows, err := db.Query(query, args...)
	if err != nil {
		rr.incDriver(batch, "error")
		log.Printf("[ramp] settle %s: %v", oppID, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var driverID, outcome string
		if err := rows.Scan(&driverID, &outcome); err != nil {
			rr.incDriver(batch, "error")
			continue
		}
		rr.incDriver(batch, strings.ToLower(outcome))
	}
	if err := rows.Err(); err != nil {
		rr.incDriver(batch, "error")
	}
}

func parseIntList(name, s string) []int {
	var out []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n <= 0 {
			log.Fatalf("invalid %s %q: %q is not a positive int", name, s, part)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		log.Fatalf("no values parsed from %s %q", name, s)
	}
	return out
}

// ── Throughput loop ───────────────────────────────────────────────────────────

func runThroughput(db *sql.DB, reg *lt.Reg, cte *cteReg, lat *latReg) {
	now := time.Now()
	winStart := now.Add(-time.Second)
	winEnd := now.Add(4 * time.Hour)

	log.Printf("[throughput] creating %d contended opps...", *contendedOpps)
	cOpps, err := setupOpps(db, "lt-c", *contendedOpps, *capacity, winStart, winEnd)
	if err != nil {
		log.Fatalf("setup contended: %v", err)
	}
	log.Printf("[throughput] creating %d diverse opps...", *diverseOpps)
	dOpps, err := setupOpps(db, "lt-d", *diverseOpps, *capacity, winStart, winEnd)
	if err != nil {
		log.Fatalf("setup diverse: %v", err)
	}
	log.Printf("[throughput] opps ready")

	dupFrac := *dupPct / 100.0
	perOpp := *contendedPerOpp

	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for range tick.C {
			go tickContended(db, cOpps, perOpp, dupFrac, reg, cte, lat)
		}
	}()

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for range tick.C {
		go tickDiverse(db, dOpps, dupFrac, reg, cte, lat)
	}
}

// ── Opp helpers ───────────────────────────────────────────────────────────────

type oppState struct {
	id   string
	mu   sync.Mutex
	sent []driverPair
}

type driverPair struct{ id, iemKey string }

const maxSent = 500

func (o *oppState) nextDrivers(count int, dupFrac float64) []driverPair {
	dupN := int(float64(count)*dupFrac + 0.5)
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.sent) == 0 {
		dupN = 0
	}
	uniqueN := count - dupN
	out := make([]driverPair, 0, count)
	for i := 0; i < uniqueN; i++ {
		id, key := newDriver()
		o.sent = append(o.sent, driverPair{id, key})
		out = append(out, driverPair{id, key})
	}
	if len(o.sent) > maxSent {
		o.sent = o.sent[len(o.sent)-maxSent:]
	}
	for i := 0; i < dupN; i++ {
		d := o.sent[rand.Intn(len(o.sent))]
		out = append(out, d)
	}
	return out
}

func (o *oppState) nextDriver(dupProb float64) driverPair {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.sent) > 0 && rand.Float64() < dupProb {
		return o.sent[rand.Intn(len(o.sent))]
	}
	id, key := newDriver()
	o.sent = append(o.sent, driverPair{id, key})
	if len(o.sent) > maxSent {
		o.sent = o.sent[1:]
	}
	return driverPair{id, key}
}

func insertOpp(db *sql.DB, oppID string, cap int) error {
	now := time.Now()
	_, err := db.Exec(
		`INSERT INTO opportunities
		   (opportunity_id, region_id, zone_id, booking_window_start, booking_window_end, capacity, remaining)
		 VALUES ($1, $2, $3, $4, $5, $6, $6)`,
		oppID, *region, *zone, now.Add(-time.Second), now.Add(4*time.Hour), cap,
	)
	return err
}

func setupOpps(db *sql.DB, prefix string, n, cap int, winStart, winEnd time.Time) ([]*oppState, error) {
	ts := time.Now().UnixMilli()
	opps := make([]*oppState, n)
	for i := range opps {
		opps[i] = &oppState{id: fmt.Sprintf("%s-%04d-%d", prefix, i, ts)}
	}
	sem := make(chan struct{}, 50)
	errc := make(chan error, n)
	var wg sync.WaitGroup
	for _, o := range opps {
		wg.Add(1)
		sem <- struct{}{}
		o := o
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			_, err := db.Exec(
				`INSERT INTO opportunities
				   (opportunity_id, region_id, zone_id, booking_window_start, booking_window_end, capacity, remaining)
				 VALUES ($1, $2, $3, $4, $5, $6, $6)`,
				o.id, *region, *zone, winStart, winEnd, cap,
			)
			if err != nil {
				errc <- fmt.Errorf("insert %s: %w", o.id, err)
			}
		}()
	}
	wg.Wait()
	close(errc)
	if err := <-errc; err != nil {
		return nil, err
	}
	return opps, nil
}

// ── Tick functions ────────────────────────────────────────────────────────────

func tickContended(db *sql.DB, opps []*oppState, perOpp int, dupFrac float64, reg *lt.Reg, cte *cteReg, lat *latReg) {
	var wg sync.WaitGroup
	for _, o := range opps {
		wg.Add(1)
		o := o
		go func() {
			defer wg.Done()
			drivers := o.nextDrivers(perOpp, dupFrac)
			start := time.Now()
			settleMulti(db, o.id, drivers, reg, cte, "contended")
			lat.record("contended", time.Since(start))
		}()
	}
	wg.Wait()
}

func tickDiverse(db *sql.DB, opps []*oppState, dupFrac float64, reg *lt.Reg, cte *cteReg, lat *latReg) {
	var wg sync.WaitGroup
	for _, o := range opps {
		wg.Add(1)
		o := o
		go func() {
			defer wg.Done()
			d := o.nextDriver(dupFrac)
			start := time.Now()
			settleMulti(db, o.id, []driverPair{d}, reg, cte, "diverse")
			lat.record("diverse", time.Since(start))
		}()
	}
	wg.Wait()
}

// ── Settle helpers ────────────────────────────────────────────────────────────

// settleOne fires a single-driver CTE (oversell test: 1 goroutine = 1 claim).
func settleOne(db *sql.DB, oppID, driverID, iemKey string, reg *lt.Reg, cte *cteReg, scenario string) {
	settleMulti(db, oppID, []driverPair{{driverID, iemKey}}, reg, cte, scenario)
}

// settleMulti runs the bulk-settle CTE for one opportunity.
func settleMulti(db *sql.DB, oppID string, drivers []driverPair, reg *lt.Reg, cte *cteReg, scenario string) {
	if len(drivers) == 0 {
		return
	}
	cte.inc(scenario)
	// Deduplicate by driver ID.
	seen := make(map[string]struct{}, len(drivers))
	unique := drivers[:0:len(drivers)]
	for _, d := range drivers {
		if _, dup := seen[d.id]; !dup {
			seen[d.id] = struct{}{}
			unique = append(unique, d)
		}
	}

	query, args := buildSettleSQL(oppID, unique)
	rows, err := db.Query(query, args...)
	if err != nil {
		reg.Inc(scenario, "error")
		log.Printf("[%s] settle %s: %v", scenario, oppID, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var driverID, outcome string
		if err := rows.Scan(&driverID, &outcome); err != nil {
			reg.Inc(scenario, "error")
			continue
		}
		reg.Inc(scenario, strings.ToLower(outcome))
	}
	if err := rows.Err(); err != nil {
		reg.Inc(scenario, "error")
	}
}

// buildSettleSQL generates the bulk-settle CTE (mirrors PgClaimStore.buildSql).
func buildSettleSQL(oppID string, drivers []driverPair) (string, []any) {
	n := len(drivers)
	args := make([]any, 0, n*2+4)
	var vals strings.Builder
	for i, d := range drivers {
		if i > 0 {
			vals.WriteString(",")
		}
		p := i*2 + 1
		fmt.Fprintf(&vals, "($%d,$%d,%d)", p, p+1, i+1)
		args = append(args, d.id, d.iemKey)
	}
	base := n*2 + 1
	for i := 0; i < 4; i++ {
		args = append(args, oppID)
	}

	query := fmt.Sprintf(`
WITH input(driver_id, idempotency_key, pos) AS (VALUES %s),
existing AS (
  SELECT i.driver_id FROM input i
  JOIN bookings b ON b.opportunity_id = $%d AND b.driver_id = i.driver_id
),
cap AS (
  SELECT CASE WHEN now() BETWEEN booking_window_start AND booking_window_end
         THEN remaining ELSE 0 END AS remaining
  FROM opportunities WHERE opportunity_id = $%d FOR UPDATE
),
cand AS (
  SELECT driver_id, idempotency_key, row_number() OVER (ORDER BY pos) AS rn
  FROM input WHERE driver_id NOT IN (SELECT driver_id FROM existing)
),
adm AS (
  SELECT driver_id, idempotency_key FROM cand CROSS JOIN cap WHERE rn <= cap.remaining
),
ins AS (
  INSERT INTO bookings (opportunity_id, driver_id, idempotency_key, status)
  SELECT $%d, driver_id, idempotency_key, 'CONFIRMED' FROM adm
  ON CONFLICT (opportunity_id, driver_id) DO NOTHING
  RETURNING driver_id
),
upd AS (
  UPDATE opportunities SET remaining = remaining - (SELECT count(*) FROM ins)
  WHERE opportunity_id = $%d RETURNING 1
)
SELECT i.driver_id,
  CASE
    WHEN i.driver_id IN (SELECT driver_id FROM ins)      THEN 'COMMITTED'
    WHEN i.driver_id IN (SELECT driver_id FROM existing) THEN 'DUPLICATE'
    ELSE 'REJECTED'
  END
FROM (SELECT DISTINCT driver_id FROM input) i`,
		vals.String(), base, base+1, base+2, base+3)

	return query, args
}
