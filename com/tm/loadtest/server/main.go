// Command server is a full-flow load test: HTTP server → Redis gate → Kafka → PG.
// Unlike redis-counter (gate in isolation) and pg (CTE in isolation), this test
// exercises the entire stack end-to-end via POST /opportunities/:id/bookings.
//
// ── ramp ─────────────────────────────────────────────────────────────────────
// Two patterns in sequence, each ramping RPS through the same levels:
//
//	contended: 1 opp, increasing RPS → hot Redis key + server bottleneck
//	diverse:   N opps round-robin    → throughput with spread load
//
// Stops when error%>5% or p99>200ms and reports the ceiling RPS.
//
// ── gate ─────────────────────────────────────────────────────────────────────
// Three-phase correctness check via HTTP (mirrors redis-counter gate test):
//
//	seed: fire seedN unique drivers  → expect 202 ACCEPTED
//	dup:  re-fire seedN drivers      → expect 200 DUP
//	rest: fire restN new drivers     → first (cap-seedN) ACCEPTED, rest 409 FULL
//
// Assert: accepted == capacity (no oversell at gate level), dup == dupN,
//
//	full == restN - (capacity-seedN), errors == 0.
//
// ── idempotent ───────────────────────────────────────────────────────────────
// Sends batch B of unique drivers, then re-sends the same batch N times.
// Assert: every re-send returns 200 DUP (Redis gate dedup working end-to-end).
//
// ── throughput ───────────────────────────────────────────────────────────────
// Sustained contended + diverse scenarios running indefinitely; metrics on /metrics.
//
// ── correctness ──────────────────────────────────────────────────────────────
// gate → idempotent; exits non-zero on any violation. Terminates on its own.
//
// ── full ─────────────────────────────────────────────────────────────────────
// gate → idempotent → ramp; exits non-zero on violation or if ramp skipped due
// to failures. Terminates on its own — suitable for `make loadtest-server`.
//
// ── all ──────────────────────────────────────────────────────────────────────
// gate → idempotent (exits non-zero on violation) → throughput (runs forever).
//
// Usage:
//
//	bazel run //com/tm/loadtest/server -- \
//	  --server=http://localhost:8080 \
//	  --run=all
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tm/loadtest/lt"
)

// ── Flags ─────────────────────────────────────────────────────────────────────

var (
	serverURL   = flag.String("server", "http://localhost:8080", "booking server base URL")
	metricsAddr = flag.String("metrics-addr", ":9412", "Prometheus /metrics listen address")
	runMode     = flag.String("run", "throughput", "test mode: ramp | gate | idempotent | throughput | correctness | full | all")

	capacity = flag.Int("capacity", 1_000, "opportunity capacity")
	workers  = flag.Int("workers", 500, "HTTP worker goroutines / connection pool size")
	region   = flag.String("region", "lt", "region_id for created opportunities")
	zone     = flag.String("zone", "lt", "zone_id for created opportunities")

	// ramp
	rampStepDur  = flag.Duration("ramp-step", 2*time.Minute, "duration per ramp step")
	rampCooldown = flag.Duration("ramp-cooldown", 15*time.Second, "pause between ramp patterns")

	// gate
	gateReqs   = flag.Int("gate-requests", 10_000, "total requests for gate test")
	gateRPS    = flag.Int("gate-rps", 2_000, "RPS for gate and idempotent tests")
	dupNFlag   = flag.Int("dup-n", 0, "exact dup count (overrides --dup-pct when > 0)")
	dupPct     = flag.Float64("dup-pct", 5.0, "percent of gate requests that are dup re-sends")
	gateRounds = flag.Int("gate-rounds", 1, "repeat gate test N times (each round uses a fresh opp)")
	disableCB  = flag.Bool("disable-cb", false, "assert throttled=0 in gate test (set when server runs with DISABLE_CIRCUIT_BREAKER=true)")

	// idempotent
	idempotentBatch  = flag.Int("idempotent-batch", 100, "distinct drivers per idempotent round")
	idempotentRounds = flag.Int("idempotent-rounds", 3, "times to replay the same batch")

	// throughput
	contendedOpps    = flag.Int("contended-opps", 10, "opps for contended scenario")
	contendedPerOpp  = flag.Int("contended-per-opp", 50, "drivers per opp per tick (contended)")
	diverseOpps      = flag.Int("diverse-opps", 500, "opps for diverse scenario")
	throughputDupPct = flag.Float64("throughput-dup-pct", 5.0, "dup % in throughput mode")
	throughputCap    = flag.Int("throughput-capacity", 10_000_000, "opp capacity in throughput mode (large to avoid FULL)")
)

// ── Violations ────────────────────────────────────────────────────────────────

var (
	oversellViolations   atomic.Int64
	idempotentViolations atomic.Int64
)

// ── Latency histogram ─────────────────────────────────────────────────────────

type latHist struct {
	mu      sync.Mutex
	samples []int64 // nanoseconds
}

func (h *latHist) record(d time.Duration) {
	h.mu.Lock()
	h.samples = append(h.samples, d.Nanoseconds())
	h.mu.Unlock()
}

func (h *latHist) stats() (p50, p99, p999, max time.Duration) {
	h.mu.Lock()
	s := append([]int64(nil), h.samples...)
	h.mu.Unlock()
	if len(s) == 0 {
		return
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	n := len(s)
	p50 = time.Duration(s[int(float64(n-1)*0.50)])
	p99 = time.Duration(s[int(float64(n-1)*0.99)])
	p999 = time.Duration(s[int(float64(n-1)*0.999)])
	max = time.Duration(s[n-1])
	return
}

// ── Per-scenario latency gauges ───────────────────────────────────────────────

type latReg struct {
	mu   sync.Mutex
	vals map[string][3]float64 // scenario → [p50s, p99s, maxs]
}

func newLatReg() *latReg { return &latReg{vals: make(map[string][3]float64)} }

func (r *latReg) set(scenario string, p50, p99, max time.Duration) {
	r.mu.Lock()
	r.vals[scenario] = [3]float64{p50.Seconds(), p99.Seconds(), max.Seconds()}
	r.mu.Unlock()
}

func (r *latReg) text() string {
	r.mu.Lock()
	snap := make(map[string][3]float64, len(r.vals))
	for k, v := range r.vals {
		snap[k] = v
	}
	r.mu.Unlock()
	if len(snap) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, pair := range []struct{ name, help string }{
		{"loadtest_server_latency_p50_seconds", "HTTP claim p50 latency"},
		{"loadtest_server_latency_p99_seconds", "HTTP claim p99 latency"},
		{"loadtest_server_latency_max_seconds", "HTTP claim max latency"},
	} {
		fmt.Fprintf(&sb, "# HELP %s %s\n# TYPE %s gauge\n", pair.name, pair.help, pair.name)
		for scenario, v := range snap {
			fmt.Fprintf(&sb, "%s{scenario=%q} %g\n", pair.name, scenario, v[i])
		}
	}
	return sb.String()
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	client := lt.NewClient(*workers)

	// smoke-test connectivity
	resp, err := client.Get(*serverURL + "/health")
	if err != nil {
		log.Fatalf("server unreachable at %s: %v", *serverURL, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("health check failed: %d", resp.StatusCode)
	}
	log.Printf("server OK: %s", *serverURL)

	reg := lt.NewReg("loadtest_server_claims_total",
		"Full-flow HTTP claim outcomes by scenario and result")
	lats := newLatReg()

	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, reg.Text())
		fmt.Fprint(w, lats.text())
		fmt.Fprintf(w,
			"# HELP loadtest_server_oversell_violations_total Gate oversell violations (accepted > capacity)\n"+
				"# TYPE loadtest_server_oversell_violations_total counter\n"+
				"loadtest_server_oversell_violations_total %d\n",
			oversellViolations.Load())
		fmt.Fprintf(w,
			"# HELP loadtest_server_idempotent_violations_total Idempotent violations (dup re-send not returned DUP)\n"+
				"# TYPE loadtest_server_idempotent_violations_total counter\n"+
				"loadtest_server_idempotent_violations_total %d\n",
			idempotentViolations.Load())
	})
	go func() {
		log.Printf("metrics on http://localhost%s/metrics", *metricsAddr)
		if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
			log.Fatalf("metrics server: %v", err)
		}
	}()
	time.Sleep(50 * time.Millisecond)

	pass := true
	switch *runMode {
	case "ramp":
		runRamp(client, reg, lats)
	case "gate":
		pass = runGate(client, reg)
	case "idempotent":
		pass = runIdempotent(client, reg)
	case "throughput":
		runThroughput(client, reg, lats)
	case "correctness":
		pass = runGate(client, reg) && pass
		pass = runIdempotent(client, reg) && pass
	case "full":
		pass = runGate(client, reg) && pass
		pass = runIdempotent(client, reg) && pass
		if pass {
			runRamp(client, reg, lats)
		}
	case "all":
		pass = runGate(client, reg) && pass
		pass = runIdempotent(client, reg) && pass
		if pass {
			runThroughput(client, reg, lats)
		}
	default:
		log.Fatalf("unknown --run=%q; choose: ramp | gate | idempotent | throughput | correctness | full | all", *runMode)
	}

	if !pass {
		os.Exit(1)
	}
}

// ── RAMP TEST ─────────────────────────────────────────────────────────────────

var rampLevels = []int{1_000, 2_000, 5_000, 10_000}

func runRamp(client *http.Client, reg *lt.Reg, lats *latReg) {
	now := time.Now()
	winStart := now.Add(-time.Second).Unix()
	winEnd := now.Add(4 * time.Hour).Unix()

	runPattern := func(name string, getOppID func(i int) string, setup func() error) {
		fmt.Printf("\n=== pattern: %s (%s/step) ===\n", name, *rampStepDur)
		fmt.Printf("%-8s  %-8s  %-6s  %-12s  %-12s  %-12s  %s\n",
			"target", "actual", "sent", "p50", "p99", "max", "error%")
		fmt.Println(strings.Repeat("─", 78))
		if err := setup(); err != nil {
			log.Printf("[ramp/%s] setup: %v", name, err)
			return
		}
		ceiling := 0
		for _, target := range rampLevels {
			scenario := fmt.Sprintf("ramp_%s_%d", name, target)
			res := rampStep(client, *serverURL, getOppID, target, *rampStepDur, reg, scenario)
			lats.set(scenario, res.p50, res.p99, res.max)

			errPct := 0.0
			if res.sent > 0 {
				errPct = float64(res.errors) * 100 / float64(res.sent)
			}
			suffix := ""
			if errPct > 5 || res.p99 > 200*time.Millisecond {
				suffix = "  ← CEILING"
			} else {
				ceiling = res.actualRPS
			}
			fmt.Printf("%-8d  %-8d  %-6d  %-12s  %-12s  %-12s  %.1f%%%s\n",
				target, res.actualRPS, res.sent,
				res.p50.Round(time.Millisecond),
				res.p99.Round(time.Millisecond),
				res.max.Round(time.Millisecond),
				errPct, suffix)
			if suffix != "" {
				break
			}
		}
		fmt.Println(strings.Repeat("─", 78))
		fmt.Printf("Max sustainable RPS (error<5%%, p99<200ms): ~%d\n", ceiling)
	}

	// contended: all requests go to 1 opp
	var contendedOppID string
	runPattern("contended",
		func(_ int) string { return contendedOppID },
		func() error {
			contendedOppID = fmt.Sprintf("lt-ramp-c-%d", time.Now().UnixMilli())
			return lt.CreateOpp(client, *serverURL, contendedOppID, *region, *zone,
				10_000_000, winStart, winEnd)
		},
	)

	log.Printf("[ramp] cooldown %s...", *rampCooldown)
	time.Sleep(*rampCooldown)

	// diverse: requests round-robin across N opps
	var diversePool []*lt.OppPool
	var diverseIdx atomic.Int64
	runPattern("diverse",
		func(_ int) string {
			i := int(diverseIdx.Add(1)-1) % len(diversePool)
			return diversePool[i].ID
		},
		func() error {
			var err error
			diversePool, err = lt.CreateOpps(client, *serverURL, "lt-ramp-d", *region, *zone,
				100, 10_000_000, winStart, winEnd)
			return err
		},
	)

	log.Printf("[ramp] cooldown %s...", *rampCooldown)
	time.Sleep(*rampCooldown)

	// realistic: fresh opp (capacity=*capacity, e.g. 1000) per step.
	// Models a real booking window: opp fills in the first seconds, then the
	// remaining ~99% of the step is pure fast-reject (SCARD≥capacity → FULL).
	// Ceiling here = max RPS the Redis gate can sustain on FULL-path rejection.
	fmt.Printf("\n=== pattern: realistic (capacity=%d, %s/step) ===\n", *capacity, *rampStepDur)
	fmt.Printf("%-8s  %-8s  %-6s  %-12s  %-12s  %-12s  %s\n",
		"target", "actual", "sent", "p50", "p99", "max", "error%")
	fmt.Println(strings.Repeat("─", 78))
	realisticCeiling := 0
	for _, target := range rampLevels {
		oppID := fmt.Sprintf("lt-ramp-r-%d-%d", target, time.Now().UnixMilli())
		if err := lt.CreateOpp(client, *serverURL, oppID, *region, *zone,
			*capacity, winStart, winEnd); err != nil {
			log.Printf("[ramp/realistic] create opp: %v", err)
			break
		}
		scenario := fmt.Sprintf("ramp_realistic_%d", target)
		res := rampStep(client, *serverURL, func(_ int) string { return oppID },
			target, *rampStepDur, reg, scenario)
		lats.set(scenario, res.p50, res.p99, res.max)

		errPct := 0.0
		if res.sent > 0 {
			errPct = float64(res.errors) * 100 / float64(res.sent)
		}
		suffix := ""
		if errPct > 5 || res.p99 > 200*time.Millisecond {
			suffix = "  ← CEILING"
		} else {
			realisticCeiling = res.actualRPS
		}
		fmt.Printf("%-8d  %-8d  %-6d  %-12s  %-12s  %-12s  %.1f%%%s\n",
			target, res.actualRPS, res.sent,
			res.p50.Round(time.Millisecond),
			res.p99.Round(time.Millisecond),
			res.max.Round(time.Millisecond),
			errPct, suffix)

		// Wait for manager to drain Kafka and settle to PG, then verify.
		log.Printf("[ramp/realistic] waiting 10s for consumer to settle...")
		time.Sleep(10 * time.Second)
		verifyOpp(client, oppID, *capacity)

		if suffix != "" {
			break
		}
	}
	fmt.Println(strings.Repeat("─", 78))
	fmt.Printf("Max sustainable RPS (error<5%%, p99<200ms): ~%d\n", realisticCeiling)

	log.Printf("[ramp] done")
}

type stepResult struct {
	sent, actualRPS, errors int
	p50, p99, max           time.Duration
}

func rampStep(client *http.Client, target string, getOppID func(int) string,
	rps int, dur time.Duration, reg *lt.Reg, scenario string) stepResult {

	const batchMs = 10
	batchSize := rps * batchMs / 1000
	if batchSize < 1 {
		batchSize = 1
	}

	work := make(chan lt.Req, batchSize*2)
	var errs atomic.Int64
	var lats latHist

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range work {
				t := time.Now()
				status, body := lt.SendClaim(client, target, r)
				lats.record(time.Since(t))
				label := lt.Label(status, body)
				// "throttled" (503) = server-side failure (Kafka overflow, Redis error,
				// or CB open). All three mean the request was NOT successfully handled —
				// count as errors for ramp ceiling detection.
				if label == "error" || label == "throttled" {
					errs.Add(1)
				}
				reg.Inc(scenario, label)
			}
		}()
	}

	ticker := time.NewTicker(batchMs * time.Millisecond)
	deadline := time.Now().Add(dur)
	start := time.Now()
	sent := 0
	i := 0
	for time.Now().Before(deadline) {
		<-ticker.C
		for j := 0; j < batchSize; j++ {
			id, key := lt.NewDriver()
			work <- lt.Req{OppID: getOppID(i), DriverID: id, IdemKey: key}
			sent++
			i++
		}
	}
	ticker.Stop()
	close(work)
	wg.Wait()
	elapsed := time.Since(start)

	p50, p99, _, max := lats.stats()
	return stepResult{
		sent:      sent,
		actualRPS: int(float64(sent) / elapsed.Seconds()),
		errors:    int(errs.Load()),
		p50:       p50, p99: p99, max: max,
	}
}

// ── GATE TEST ─────────────────────────────────────────────────────────────────

// gateRoundPause must exceed the server's CB waitDurationInOpenState (5s) so
// a CB trip during one round/warmup is fully recovered before the next starts.
// 30s gives Redis connection pool and CB sliding window time to fully stabilise
// even on slow container starts; can be reduced once the root cause is confirmed.
const gateRoundPause = 30 * time.Second

func runGate(client *http.Client, reg *lt.Reg) bool {
	fmt.Println("\n══ GATE TEST ════════════════════════════════════════════════════════")
	fmt.Printf("rounds=%d  capacity=%d  requests=%d  dup=%.0f%%  rps=%d\n\n",
		*gateRounds, *capacity, *gateReqs, *dupPct, *gateRPS)

	// Warmup: fill one throwaway opp at full RPS to prime HTTP connections,
	// Redis client pool, and server circuit breaker before the real rounds start.
	fmt.Printf("[warmup] priming connections at %d rps...\n", *gateRPS)
	now0 := time.Now()
	wID := fmt.Sprintf("lt-gate-warmup-%d", now0.UnixMilli())
	if err := lt.CreateOpp(client, *serverURL, wID, *region, *zone,
		*capacity, now0.Add(-time.Second).Unix(), now0.Add(time.Hour).Unix()); err == nil {
		fireHTTP(client, *serverURL, lt.BuildReqs(wID, *capacity, 0),
			*gateRPS, &tally{}, &latHist{}, reg, "gate_warmup")
	}
	// Sleep > waitDurationInOpenState (5s) so the CB is out of OPEN state before
	// round 1 starts. Without this, the dup phase in round 1 gets THROTTLED
	// instead of DUP when the CB trips during warmup.
	time.Sleep(gateRoundPause)
	fmt.Println()

	allOk := true
	for round := 1; round <= *gateRounds; round++ {
		if *gateRounds > 1 {
			fmt.Printf("── round %d/%d ──────────────────────────────────────────────────\n", round, *gateRounds)
		}
		allOk = gateRound(client, reg, round) && allOk
		// Pause between rounds: let the circuit breaker recover and Kafka
		// consumer drain the previous round's messages before the next seed.
		if round < *gateRounds {
			time.Sleep(gateRoundPause)
		}
	}
	fmt.Println()
	return allOk
}

func calcDupN(total int) int {
	if *dupNFlag > 0 {
		return *dupNFlag
	}
	return int(float64(total)**dupPct/100 + 0.5)
}

type tally struct {
	accepted, dup, full, closed, throttled, errCount atomic.Int64
}

func (t *tally) add(label string) {
	switch label {
	case "accepted":
		t.accepted.Add(1)
	case "dup":
		t.dup.Add(1)
	case "full":
		t.full.Add(1)
	case "closed":
		t.closed.Add(1)
	case "throttled":
		t.throttled.Add(1)
	default:
		t.errCount.Add(1)
	}
}

func (t *tally) total() int64 {
	return t.accepted.Load() + t.dup.Load() + t.full.Load() +
		t.closed.Load() + t.throttled.Load() + t.errCount.Load()
}

func gateRound(client *http.Client, reg *lt.Reg, round int) bool {
	now := time.Now()
	oppID := fmt.Sprintf("lt-gate-%d-%d", round, now.UnixMilli())
	if err := lt.CreateOpp(client, *serverURL, oppID, *region, *zone,
		*capacity, now.Add(-time.Second).Unix(), now.Add(4*time.Hour).Unix()); err != nil {
		fmt.Printf("✗ setup failed: %v\n\n", err)
		return false
	}

	dupN := calcDupN(*gateReqs)
	seedN := dupN
	// Seed must be strictly less than capacity: the Redis gate checks SCARD>=capacity
	// before SISMEMBER (intentional — fast reject when full). If seed fills the opp,
	// the dup phase would also get FULL instead of DUP.
	if seedN >= *capacity {
		seedN = *capacity / 2
		dupN = seedN
	}
	restN := *gateReqs - seedN - dupN

	// seed: unique drivers → all should be ACCEPTED
	seedReqs := lt.BuildReqs(oppID, seedN, 0)
	// dup: re-fire seed drivers → all should be DUP (opp not full yet, SISMEMBER runs)
	dupReqs := make([]lt.Req, dupN)
	for i := range dupReqs {
		dupReqs[i] = seedReqs[i%seedN]
	}
	// rest: new unique drivers → (cap-seedN) ACCEPTED, remainder FULL
	restReqs := lt.BuildReqs(oppID, restN, 0)

	scenario := fmt.Sprintf("gate_r%d", round)
	// Use separate tallies per phase so we can compute effectiveSeedN: the number of
	// drivers that actually entered claimed_set. If the CB trips during the seed phase,
	// some seed requests are throttled before SADD runs — those drivers are never
	// deduplicated. effectiveSeedN drives the dup and rest assertions.
	var seedTal, dupTal, restTal tally
	var lh latHist

	fireHTTP(client, *serverURL, seedReqs, *gateRPS, &seedTal, &lh, reg, scenario)
	fireHTTP(client, *serverURL, dupReqs, *gateRPS, &dupTal, &lh, reg, scenario)
	fireHTTP(client, *serverURL, restReqs, *gateRPS, &restTal, &lh, reg, scenario)

	p50, p99, _, _ := lh.stats()
	fmt.Printf("latency  p50=%s  p99=%s\n", p50.Round(time.Microsecond), p99.Round(time.Microsecond))

	// effectiveSeedN: drivers actually SADD'd during seed (< seedN when CB throttled some).
	// dupTal.accepted: throttled-seed drivers that got SADD'd on their dup-phase replay.
	// claimedBeforeRest: total members in claimed_set when the rest phase starts.
	effectiveSeedN := seedTal.accepted.Load()
	claimedBeforeRest := effectiveSeedN + dupTal.accepted.Load()

	la := seedTal.accepted.Load() + dupTal.accepted.Load() + restTal.accepted.Load()
	lf := seedTal.full.Load() + dupTal.full.Load() + restTal.full.Load()
	ld := dupTal.dup.Load()
	// Only dup+rest throttled count against wantFull; seed throttled reduced effectiveSeedN
	// and are already accounted for via claimedBeforeRest.
	throttledAfterSeed := dupTal.throttled.Load() + restTal.throttled.Load()
	totalThrottled := seedTal.throttled.Load() + throttledAfterSeed
	totalErrors := seedTal.errCount.Load() + dupTal.errCount.Load() + restTal.errCount.Load()
	totalGot := seedTal.total() + dupTal.total() + restTal.total()

	// full + (dup+rest throttled) = slots rejected after opp was full
	wantFull := int64(restN) - (int64(*capacity) - claimedBeforeRest)

	fmt.Printf("%-35s  %8s  %8s\n", "", "got", "want")
	fmt.Printf("%-35s  %8d  %8d\n", "accepted (202)", la, int64(*capacity))
	fmt.Printf("%-35s  %8d  %8d\n", "full (409)", lf, wantFull)
	fmt.Printf("%-35s  %8d  %8d  [=effectiveSeedN]\n", "dup (200)", ld, effectiveSeedN)
	if totalThrottled > 0 {
		fmt.Printf("%-35s  %8d         [CB open: seed=%d dup+rest=%d]\n",
			"throttled (503)", totalThrottled, seedTal.throttled.Load(), throttledAfterSeed)
	}
	if totalErrors > 0 {
		fmt.Printf("%-35s  %8d\n", "errors (other)", totalErrors)
	}
	if seedTal.throttled.Load() > 0 {
		fmt.Printf("  note: %d seed throttled → claimedBeforeRest=%d (not %d)\n",
			seedTal.throttled.Load(), claimedBeforeRest, seedN)
	}
	fmt.Println()

	ok := true
	ok = check("no oversell: accepted == capacity", la == int64(*capacity)) && ok
	// full + throttled(dup+rest) = all rejects after opp was full. Seed throttled are
	// excluded because they shrink claimedBeforeRest, which already adjusts wantFull.
	ok = check("full+throttled(dup+rest) == restN-(cap-claimedBeforeRest)", lf+throttledAfterSeed == wantFull) && ok
	// Every driver in claimed_set (effectiveSeedN) must return DUP on replay.
	ok = check("dup == effectiveSeedN", ld == effectiveSeedN) && ok
	ok = check("no connection errors", totalErrors == 0) && ok
	ok = check("no lost responses: total == sent", totalGot == int64(*gateReqs)) && ok
	if *disableCB {
		ok = check("CB disabled: throttled == 0", totalThrottled == 0) && ok
	}

	if la > int64(*capacity) {
		oversellViolations.Add(1)
	}
	ok = verifyOpp(client, oppID, *capacity) && ok
	return ok
}

// ── IDEMPOTENT TEST ───────────────────────────────────────────────────────────

func runIdempotent(client *http.Client, reg *lt.Reg) bool {
	fmt.Println("\n══ IDEMPOTENT TEST ══════════════════════════════════════════════════")
	batchSize := *idempotentBatch
	rounds := *idempotentRounds
	// Opp capacity must exceed batch size so that after round 0 (batch accepted),
	// SCARD < capacity and SISMEMBER runs on replays → DUP instead of FULL.
	cap := *capacity
	if cap <= batchSize {
		cap = batchSize * 5
	}

	fmt.Printf("capacity=%d  batch=%d  replay-rounds=%d  rps=%d\n\n",
		cap, batchSize, rounds, *gateRPS)

	now := time.Now()
	oppID := fmt.Sprintf("lt-idem-%d", now.UnixMilli())
	if err := lt.CreateOpp(client, *serverURL, oppID, *region, *zone,
		cap, now.Add(-time.Second).Unix(), now.Add(4*time.Hour).Unix()); err != nil {
		fmt.Printf("✗ setup failed: %v\n\n", err)
		return false
	}

	// Round 0: unique batch → all should be ACCEPTED (202)
	batch := lt.BuildReqs(oppID, batchSize, 0)

	var tal0 tally
	fireHTTP(client, *serverURL, batch, *gateRPS, &tal0, &latHist{}, reg, "idempotent_r0")
	fmt.Printf("round 0: accepted=%d  dup=%d  full=%d  throttled=%d  errors=%d\n",
		tal0.accepted.Load(), tal0.dup.Load(), tal0.full.Load(), tal0.throttled.Load(), tal0.errCount.Load())

	ok := check("round 0: all accepted (202)", tal0.accepted.Load() == int64(batchSize))

	// Rounds 1..N: re-send same batch → all should be DUP (200)
	allOk := ok
	for round := 1; round <= rounds; round++ {
		var talR tally
		fireHTTP(client, *serverURL, batch, *gateRPS, &talR, &latHist{}, reg, fmt.Sprintf("idempotent_r%d", round))
		fmt.Printf("round %d: accepted=%d  dup=%d  full=%d  throttled=%d  errors=%d\n",
			round, talR.accepted.Load(), talR.dup.Load(), talR.full.Load(), talR.throttled.Load(), talR.errCount.Load())

		roundOk := check(fmt.Sprintf("round %d: all dup (200)", round),
			talR.dup.Load() == int64(batchSize))
		if !roundOk {
			idempotentViolations.Add(1)
		}
		allOk = roundOk && allOk
	}

	allOk = verifyOpp(client, oppID, cap) && allOk
	fmt.Println()
	return allOk
}

// ── THROUGHPUT LOOP ───────────────────────────────────────────────────────────

func runThroughput(client *http.Client, reg *lt.Reg, lats *latReg) {
	now := time.Now()
	winStart := now.Add(-time.Second).Unix()
	winEnd := now.Add(4 * time.Hour).Unix()

	log.Printf("[throughput] creating %d contended opps...", *contendedOpps)
	cOpps, err := lt.CreateOpps(client, *serverURL, "lt-c", *region, *zone,
		*contendedOpps, *throughputCap, winStart, winEnd)
	if err != nil {
		log.Fatalf("setup contended: %v", err)
	}
	log.Printf("[throughput] creating %d diverse opps...", *diverseOpps)
	dOpps, err := lt.CreateOpps(client, *serverURL, "lt-d", *region, *zone,
		*diverseOpps, *throughputCap, winStart, winEnd)
	if err != nil {
		log.Fatalf("setup diverse: %v", err)
	}
	log.Printf("[throughput] opps ready — running indefinitely")

	dupFrac := *throughputDupPct / 100.0

	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for range tick.C {
			go tickContended(client, cOpps, *contendedPerOpp, dupFrac, reg, lats)
		}
	}()

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for range tick.C {
		go tickDiverse(client, dOpps, dupFrac, reg, lats)
	}
}

func tickContended(client *http.Client, opps []*lt.OppPool, perOpp int, dupFrac float64, reg *lt.Reg, lats *latReg) {
	var wg sync.WaitGroup
	for _, o := range opps {
		wg.Add(1)
		o := o
		go func() {
			defer wg.Done()
			reqs := o.NextReqs(perOpp, dupFrac)
			var h latHist
			fireHTTP(client, *serverURL, reqs, 0, &tally{}, &h, reg, "contended")
			if p50, p99, _, max := h.stats(); p99 > 0 {
				lats.set("contended", p50, p99, max)
			}
		}()
	}
	wg.Wait()
}

func tickDiverse(client *http.Client, opps []*lt.OppPool, dupFrac float64, reg *lt.Reg, lats *latReg) {
	var wg sync.WaitGroup
	for _, o := range opps {
		wg.Add(1)
		o := o
		go func() {
			defer wg.Done()
			r := o.NextReq(dupFrac)
			t := time.Now()
			status, body := lt.SendClaim(client, *serverURL, r)
			elapsed := time.Since(t)
			reg.Inc("diverse", lt.Label(status, body))
			lats.set("diverse", elapsed, elapsed, elapsed)
		}()
	}
	wg.Wait()
}

// ── Fire helper ───────────────────────────────────────────────────────────────

// fireHTTP sends reqs at rps (0 = unlimited) using a fixed worker pool.
func fireHTTP(client *http.Client, target string, reqs []lt.Req, rps int,
	tal *tally, lats *latHist, reg *lt.Reg, scenario string) {

	if len(reqs) == 0 {
		return
	}

	const batchMs = 10
	batchSize := 1
	if rps > 0 {
		batchSize = rps * batchMs / 1000
		if batchSize < 1 {
			batchSize = 1
		}
	}

	work := make(chan lt.Req, batchSize*4)

	nw := *workers
	var wg sync.WaitGroup
	for i := 0; i < nw; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range work {
				t := time.Now()
				status, body := lt.SendClaim(client, target, r)
				lats.record(time.Since(t))
				label := lt.Label(status, body)
				tal.add(label)
				reg.Inc(scenario, label)
			}
		}()
	}

	if rps <= 0 {
		// unlimited: feed all at once
		for _, r := range reqs {
			work <- r
		}
	} else {
		ticker := time.NewTicker(batchMs * time.Millisecond)
		i := 0
		for i < len(reqs) {
			<-ticker.C
			end := i + batchSize
			if end > len(reqs) {
				end = len(reqs)
			}
			for _, r := range reqs[i:end] {
				work <- r
			}
			i = end
		}
		ticker.Stop()
	}
	close(work)
	wg.Wait()
}

// ── Post-test verification ────────────────────────────────────────────────────

// verifyOpp calls GET /opportunities/:id, prints remaining/booked, and checks
// remaining >= 0 and remaining+booked == capacity.
// It retries up to maxWait to allow in-flight consumer commits to settle.
func verifyOpp(client *http.Client, oppID string, wantCapacity int) bool {
	const maxWait = 5 * time.Second
	const poll = 2 * time.Second

	type oppResp struct {
		Capacity  int `json:"capacity"`
		Remaining int `json:"remaining"`
	}

	fmt.Printf("\n── verify opp %s (waiting up to %s for consumer) ──\n", oppID, maxWait)
	deadline := time.Now().Add(maxWait)
	for {
		resp, err := client.Get(*serverURL + "/opportunities/" + oppID)
		if err != nil {
			fmt.Printf("  GET /opportunities/%s: %v\n", oppID, err)
		} else {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var o oppResp
			if resp.StatusCode == 200 && json.Unmarshal(body, &o) == nil {
				booked := o.Capacity - o.Remaining
				fmt.Printf("  capacity=%d  remaining=%d  booked=%d\n", o.Capacity, o.Remaining, booked)
				ok := true
				ok = check("remaining >= 0", o.Remaining >= 0) && ok
				ok = check(fmt.Sprintf("booked (%d) <= capacity (%d)", booked, o.Capacity), booked <= o.Capacity) && ok
				if wantCapacity > 0 {
					ok = check(fmt.Sprintf("remaining+booked == capacity (%d)", wantCapacity),
						o.Remaining+booked == wantCapacity) && ok
				}
				return ok
			}
		}
		if time.Now().After(deadline) {
			fmt.Printf("  ✗ timed out waiting for opportunity state\n")
			return false
		}
		time.Sleep(poll)
	}
}

// ── Assertion helper ──────────────────────────────────────────────────────────

func check(label string, ok bool) bool {
	if ok {
		fmt.Printf("  ✓ %s\n", label)
	} else {
		fmt.Printf("  ✗ %s\n", label)
	}
	return ok
}
