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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"

	"github.com/tm/loadtest/lt"
)

// ── Flags ────────────────────────────────────────────────────────────────────

var (
	pgDSN      = flag.String("pg", "postgres://earlybird:earlybird@localhost:5432/earlybird?sslmode=disable", "Postgres DSN")
	metricsAddr = flag.String("metrics-addr", ":9411", "Prometheus /metrics listen address")
	runMode     = flag.String("run", "throughput", "test mode: throughput|oversell|idempotent|ramp|all")

	// throughput flags
	contendedOpps    = flag.Int("contended-opps", 20, "opps for contended scenario")
	contendedPerOpp  = flag.Int("contended-per-opp", 50, "drivers per opp per tick (contended)")
	diverseOpps      = flag.Int("diverse-opps", 1_000, "opps for diverse scenario")
	dupPct           = flag.Float64("dup-pct", 5.0, "percent of requests that are dup re-sends (0–100)")
	capacity         = flag.Int("capacity", 10_000_000, "opportunity capacity (throughput mode)")

	// oversell flags
	oversellCapacity = flag.Int("oversell-capacity", 1_000, "capacity for oversell correctness test")
	oversellExtra    = flag.Int("oversell-extra", 500, "extra concurrent claimants beyond capacity")
	oversellInterval = flag.Duration("oversell-interval", 30*time.Second, "interval between oversell rounds")
	oversellRounds   = flag.Int("oversell-rounds", 3, "number of oversell rounds (0 = infinite)")

	// idempotent flags
	idempotentCapacity = flag.Int("idempotent-capacity", 200, "capacity for idempotent test")
	idempotentBatch    = flag.Int("idempotent-batch", 50, "distinct drivers per idempotent round")
	idempotentRounds   = flag.Int("idempotent-rounds", 5, "times to replay the same batch")

	// ramp flags
	rampStartRate = flag.Int("ramp-start-rate", 100, "request rate for step 1 (req/s)")
	rampStepSize  = flag.Int("ramp-step-size", 100, "add this many req/s each step")
	rampSteps     = flag.Int("ramp-steps", 5, "number of steps per pattern")
	rampStepDur   = flag.Duration("ramp-step-dur", time.Minute, "how long to hold each rate level")
	rampCapacity  = flag.Int("ramp-capacity", 10_000_000, "opp capacity per ramp step (large to avoid REJECTED noise)")
	rampCooldown   = flag.Duration("ramp-cooldown", 15*time.Second, "pause between patterns so Grafana shows a clear gap")
	rampBatchSize  = flag.Int("ramp-batch-size", 20, "drivers per CTE in contended pattern (mirrors manager bulk-settle)")

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
	mu   sync.Mutex
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
	db.SetMaxIdleConns(maxConns / 2)
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

	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, reg.Text())
		fmt.Fprint(w, cte.text())
		fmt.Fprint(w, lat.text())
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
		runRamp(db, reg, cte, lat)
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

// ── Ramp test ─────────────────────────────────────────────────────────────────
//
// Runs 4 patterns in sequence.  Each pattern runs --ramp-steps steps; step N
// sends (startRate + (N-1)*stepSize) req/s for --ramp-step-dur, then moves on.
// Rate is enforced by a time.Ticker so load arrives as a smooth stream.
//
//   contended  — 1 shared opp; every request targets the same FOR UPDATE row
//   diverse    — rotating opp pool; lock spread across rows
//   oversell   — 1 opp capacity=startRate/2; ~half COMMITTED rest REJECTED
//   idempotent — 1 opp; fixed driver pool replayed (dedup cost under rate)

func runRamp(db *sql.DB, reg *lt.Reg, cte *cteReg, lat *latReg) {
	log.Printf("[ramp] start=%d step-size=%d steps=%d dur=%s",
		*rampStartRate, *rampStepSize, *rampSteps, *rampStepDur)

	// rateStep runs one rate level for stepDur, collecting latency samples.
	rateStep := func(scenario string, rate int, fn func()) {
		h := &latHist{}
		interval := time.Second / time.Duration(rate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		deadline := time.NewTimer(*rampStepDur)
		defer deadline.Stop()
		var wg sync.WaitGroup
		for {
			select {
			case <-deadline.C:
				// Snapshot before waiting for in-flight goroutines.
				p50 := time.Duration(h.percentile(0.50) * float64(time.Second))
				p99 := time.Duration(h.percentile(0.99) * float64(time.Second))
				mx := time.Duration(h.max() * float64(time.Second))
				wg.Wait()
				lat.record(fmt.Sprintf("%s-%d", scenario, rate), p99)
				fmt.Printf("%-8d %-12s %-12s %-12s\n",
					rate,
					p50.Round(time.Millisecond),
					p99.Round(time.Millisecond),
					mx.Round(time.Millisecond),
				)
				return
			case <-ticker.C:
				wg.Add(1)
				go func() {
					defer wg.Done()
					t0 := time.Now()
					fn()
					h.record(time.Since(t0))
				}()
			}
		}
	}

	runPattern := func(name string, setup func(rate int) (fn func(), teardown func(), err error)) {
		fmt.Printf("\n=== pattern: %s (%d steps × %s) ===\n", name, *rampSteps, *rampStepDur)
		fmt.Printf("%-8s %-12s %-12s %-12s\n", "rate/s", "p50", "p99", "max")
		for step := 0; step < *rampSteps; step++ {
			rate := *rampStartRate + step**rampStepSize
			fn, teardown, err := setup(rate)
			if err != nil {
				log.Printf("[ramp/%s] setup step %d: %v", name, step+1, err)
				return
			}
			rateStep(name, rate, fn)
			if teardown != nil {
				teardown()
			}
		}
	}

	cooldown := func() {
		log.Printf("[ramp] cooldown %s...", *rampCooldown)
		time.Sleep(*rampCooldown)
	}

	// contended: 1 opp shared by all requests in a step.
	// contended: 1 opp, each tick settles a batch of drivers in 1 CTE.
	// rate = CTE/s; each CTE carries ramp-batch-size drivers → actual driver/s = rate×batch.
	runPattern("contended", func(rate int) (func(), func(), error) {
		oppID := fmt.Sprintf("lt-ramp-c-%d", time.Now().UnixMilli())
		if err := insertOpp(db, oppID, *rampCapacity); err != nil {
			return nil, nil, err
		}
		batch := *rampBatchSize
		return func() {
			drivers := make([]driverPair, batch)
			for i := range drivers {
				id, key := newDriver()
				drivers[i] = driverPair{id, key}
			}
			settleMulti(db, oppID, drivers, reg, cte, "ramp-contended")
		}, nil, nil
	})
	cooldown()

	// diverse: rotating pool of opps; each request picks the next opp in round-robin.
	runPattern("diverse", func(rate int) (func(), func(), error) {
		poolSize := min(rate, 200)
		ts := time.Now().UnixMilli()
		oppIDs := make([]string, poolSize)
		for i := range oppIDs {
			oppIDs[i] = fmt.Sprintf("lt-ramp-d-%d-%d", i, ts)
			if err := insertOpp(db, oppIDs[i], *rampCapacity); err != nil {
				return nil, nil, err
			}
		}
		var idx atomic.Int64
		return func() {
			i := int(idx.Add(1)-1) % poolSize
			dID, iKey := newDriver()
			settleOne(db, oppIDs[i], dID, iKey, reg, cte, "ramp-diverse")
		}, nil, nil
	})
	cooldown()

	// oversell: 1 opp capacity=rate/2; ~half requests will be REJECTED.
	runPattern("oversell", func(rate int) (func(), func(), error) {
		cap := max(rate/2, 1)
		oppID := fmt.Sprintf("lt-ramp-o-%d", time.Now().UnixMilli())
		if err := insertOpp(db, oppID, cap); err != nil {
			return nil, nil, err
		}
		return func() {
			dID, iKey := newDriver()
			settleOne(db, oppID, dID, iKey, reg, cte, "ramp-oversell")
		}, nil, nil
	})
	cooldown()

	// idempotent: fixed driver pool replayed round-robin — measures dedup cost.
	runPattern("idempotent", func(rate int) (func(), func(), error) {
		batchSize := min(rate, *idempotentCapacity)
		oppID := fmt.Sprintf("lt-ramp-i-%d", time.Now().UnixMilli())
		if err := insertOpp(db, oppID, *idempotentCapacity); err != nil {
			return nil, nil, err
		}
		pool := make([]driverPair, batchSize)
		for i := range pool {
			id, key := newDriver()
			pool[i] = driverPair{id, key}
		}
		// Pre-settle once so subsequent calls hit the duplicate path.
		settleMulti(db, oppID, pool, reg, cte, "ramp-idempotent")
		var idx atomic.Int64
		return func() {
			i := int(idx.Add(1)-1) % batchSize
			settleOne(db, oppID, pool[i].id, pool[i].iemKey, reg, cte, "ramp-idempotent")
		}, nil, nil
	})

	log.Printf("[ramp] done")
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
