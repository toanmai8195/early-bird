// Command redis-counter tests the Redis claim gate (Lua EVAL) in isolation —
// no HTTP server, no Kafka, no PG.  Three test modes:
//
// ── ramp ────────────────────────────────────────────────────────────────────
// Gradually increases RPS [500 → 1K → 2K → 5K → 10K → 20K → 50K], 5 s each
// step on a fresh opp (cap=10M so it never fills), using a fixed worker pool.
// Measures actual throughput + p50/p99 latency.  Stops when error%>5% or
// p99>100ms and reports the throughput ceiling.
//
// ── gate ────────────────────────────────────────────────────────────────────
// Correctness test for the capacity gate.  Three-way check:
//   1. Local tally  — what our goroutines observed per response label.
//   2. Redis SCARD  — ground truth: how many drivers entered claimed_set.
//   3. Prometheus   — what /metrics exposes after the run.
// All three must agree.  The test also asserts no overselling and correct
// dup/full counts.
//
// ── closed ──────────────────────────────────────────────────────────────────
// Correctness test for the pre-window reject path.  Window set in the future
// so every request must return CLOSED; accepted==0; SCARD==0.
//
// Exit code 0 = all assertions pass, 1 = any assertion failed.
//
// Usage:
//
//	bazel run //com/tm/loadtest/redis-counter -- \
//	  --redis=localhost:6379 --run=all
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tm/loadtest/lt"
)

// luaScript must match VertxClaimGate exactly — do not modify independently.
const luaScript = `local meta = redis.call('HMGET', KEYS[2], 'capacity', 'window_start')
if not meta[1] then return 'CLOSED' end
local now = tonumber(redis.call('TIME')[1])
if now < tonumber(meta[2]) then return 'CLOSED' end
if redis.call('SCARD', KEYS[1]) >= tonumber(meta[1]) then return 'FULL' end
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return 'DUP' end
redis.call('SADD', KEYS[1], ARGV[1])
return 'OK'`

// ── Flags ─────────────────────────────────────────────────────────────────────

var (
	redisAddr   = flag.String("redis", "localhost:6379", "Redis address host:port")
	metricsAddr = flag.String("metrics-addr", ":9410", "Prometheus /metrics listen address")
	run         = flag.String("run", "all", "scenarios: ramp | gate | closed | all")

	// gate flags
	capacity   = flag.Int("capacity", 1_000, "opp capacity for gate test")
	gateReqs   = flag.Int("gate-requests", 10_000, "total requests for gate test (unique+dup)")
	dupPct     = flag.Float64("dup-pct", 5.0, "percent of gate requests that are dup re-sends")
	dupN_flag  = flag.Int("dup-n", 0, "exact dup count (overrides --dup-pct when > 0)")
	gateRPS    = flag.Int("gate-rps", 2_000, "RPS for gate and closed tests")
	gateRounds = flag.Int("gate-rounds", 1, "repeat gate test N times (each round uses a fresh opp)")
	closedN    = flag.Int("closed-requests", 2_000, "total requests for closed test")

	poolSize    = flag.Int("pool", 500, "Redis connection pool size (also = worker count)")
	rampStepDur = flag.Duration("ramp-step", 5*time.Second, "duration of each ramp step")
	workers     = flag.Int("workers", 0, "worker goroutines for ramp (0 = 4×NumCPU)")
)

// ── Entry point ───────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	pool, err := newPool(*redisAddr, *poolSize)
	if err != nil {
		log.Fatalf("connect Redis %s: %v", *redisAddr, err)
	}
	log.Printf("Redis pool: %d conns → %s", *poolSize, *redisAddr)

	reg := lt.NewReg("loadtest_redis_claims_total",
		"Redis gate Lua EVAL outcomes by scenario and result")
	lats := newLatReg()

	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, reg.Text())
		fmt.Fprint(w, lats.text())
	})
	go func() {
		if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
			log.Fatalf("metrics server: %v", err)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	log.Printf("metrics on http://localhost%s/metrics", *metricsAddr)

	pass := true
	switch *run {
	case "ramp":
		rampTest(pool, reg, lats)
	case "gate":
		pass = gateTest(pool, reg)
	case "closed":
		pass = closedTest(pool, reg)
	case "stress":
		pass = stressTest(pool, reg)
	case "all":
		rampTest(pool, reg, lats)
		pass = gateTest(pool, reg) && pass
		pass = stressTest(pool, reg) && pass
		pass = closedTest(pool, reg) && pass
	default:
		log.Fatalf("unknown --run=%q; choose: ramp | gate | stress | closed | all", *run)
	}

	if !pass {
		os.Exit(1)
	}
}

// ── Latency gauge registry ────────────────────────────────────────────────────

// latReg stores p50/p99/max latency per scenario as Prometheus gauges.
// Values are set once after each step completes.
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
	for _, pair := range []struct{ name, help, idx string }{
		{"loadtest_redis_latency_p50_seconds", "Ramp step p50 EVAL latency", "0"},
		{"loadtest_redis_latency_p99_seconds", "Ramp step p99 EVAL latency", "1"},
		{"loadtest_redis_latency_max_seconds", "Ramp step max EVAL latency", "2"},
	} {
		fmt.Fprintf(&sb, "# HELP %s %s\n# TYPE %s gauge\n", pair.name, pair.help, pair.name)
		i := map[string]int{"0": 0, "1": 1, "2": 2}[pair.idx]
		for scenario, v := range snap {
			fmt.Fprintf(&sb, "%s{scenario=%q} %g\n", pair.name, scenario, v[i])
		}
	}
	return sb.String()
}

// ── RAMP TEST ─────────────────────────────────────────────────────────────────

var rampLevels = []int{500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000}

func rampTest(pool *redisPool, reg *lt.Reg, lats *latReg) {
	fmt.Println()
	fmt.Println("══ RAMP TEST ═══════════════════════════════════════════════════════")
	fmt.Printf("%-8s  %-8s  %-6s  %-10s  %-10s  %-10s  %s\n",
		"target", "actual", "sent", "p50", "p99", "max", "error%")
	fmt.Println(strings.Repeat("─", 74))

	ceiling := 0
	for _, targetRPS := range rampLevels {
		oppID := fmt.Sprintf("lt-ramp-%d", time.Now().UnixMilli())
		ck, mk := claimedKey(oppID), metaKey(oppID)
		now := time.Now().Unix()
		if err := setupOpp(pool, ck, mk, 10_000_000, now, now+7200); err != nil {
			log.Printf("[ramp] setup @%d RPS: %v", targetRPS, err)
			continue
		}

		scenario := fmt.Sprintf("ramp_%d", targetRPS)
		res := runStep(pool, ck, mk, targetRPS, *rampStepDur, reg, scenario)
		lats.set(scenario, res.p50, res.p99, res.max)

		errPct := 0.0
		if res.sent > 0 {
			errPct = float64(res.errors) * 100 / float64(res.sent)
		}

		suffix := ""
		if errPct > 5 || res.p99 > 100*time.Millisecond {
			suffix = "  ← CEILING"
		} else {
			ceiling = res.actualRPS
		}
		fmt.Printf("%-8d  %-8d  %-6d  %-10s  %-10s  %-10s  %.1f%%%s\n",
			targetRPS, res.actualRPS, res.sent,
			res.p50.Round(time.Microsecond),
			res.p99.Round(time.Microsecond),
			res.max.Round(time.Microsecond),
			errPct, suffix)

		if suffix != "" {
			break
		}
	}

	fmt.Println(strings.Repeat("─", 74))
	fmt.Printf("Max sustainable RPS (error<5%%, p99<100ms): ~%d\n\n", ceiling)
}

type stepResult struct {
	sent, actualRPS, errors int
	p50, p99, max           time.Duration
}

// errorLabel classifies a Redis error string into a Prometheus result label.
func errorLabel(err error) string {
	s := err.Error()
	if strings.Contains(s, "OOM") {
		return "err_oom"
	}
	if strings.Contains(s, "timeout") || strings.Contains(s, "i/o") {
		return "err_timeout"
	}
	return "err_other"
}

// numWorkers returns the effective worker count for ramp steps.
func numWorkers() int {
	if *workers > 0 {
		return *workers
	}
	w := runtime.NumCPU() * 4
	if w > *poolSize {
		w = *poolSize
	}
	return w
}

// runStep fires targetRPS/s for dur using a fixed worker pool fed by a
// rate-limited channel.  Stops after dur regardless of how many items were sent.
// Errors are recorded in reg under scenario so Prometheus tracks OOM/timeout separately.
func runStep(pool *redisPool, ck, mk string, targetRPS int, dur time.Duration, reg *lt.Reg, scenario string) stepResult {
	// Buffer = 2× batch so producer never blocks on a slow tick.
	const batchMs = 10
	batchSize := targetRPS * batchMs / 1000
	if batchSize < 1 {
		batchSize = 1
	}
	work := make(chan string, batchSize*2)

	var errs atomic.Int64
	var lats latHist

	nw := numWorkers()
	var wg sync.WaitGroup
	for i := 0; i < nw; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for driverID := range work {
				t := time.Now()
				c := pool.get()
				result, err := c.eval(luaScript, ck, mk, driverID)
				pool.put(c)
				lats.record(time.Since(t))
				if err != nil {
					errs.Add(1)
					reg.Inc(scenario, errorLabel(err))
				} else {
					reg.Inc(scenario, labelOf(result))
				}
			}
		}()
	}

	// Batch producer: fire every 10ms, send batchSize items per tick.
	// Stop after dur — don't wait for a fixed item count.
	ticker := time.NewTicker(batchMs * time.Millisecond)
	deadline := time.Now().Add(dur)
	start := time.Now()
	sent := 0
	for time.Now().Before(deadline) {
		<-ticker.C
		for i := 0; i < batchSize; i++ {
			id, _ := lt.NewDriver()
			work <- id
			sent++
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
		p50:       p50,
		p99:       p99,
		max:       max,
	}
}

// ── GATE CORRECTNESS TEST ─────────────────────────────────────────────────────

type tally struct {
	accepted, full, dup, closed, errCount atomic.Int64
}

func (t *tally) add(label string) {
	switch label {
	case "accepted":
		t.accepted.Add(1)
	case "full":
		t.full.Add(1)
	case "dup":
		t.dup.Add(1)
	case "closed":
		t.closed.Add(1)
	default:
		t.errCount.Add(1)
	}
}

func (t *tally) total() int64 {
	return t.accepted.Load() + t.full.Load() + t.dup.Load() +
		t.closed.Load() + t.errCount.Load()
}

// gateTest runs --gate-rounds rounds of the capacity gate correctness check.
// Each round uses a fresh opp so claimed_set starts empty.
// Three-way check per round: local tally vs Redis SCARD vs Prometheus /metrics.
func gateTest(pool *redisPool, reg *lt.Reg) bool {
	fmt.Println("══ GATE TEST ════════════════════════════════════════════════════════")
	fmt.Printf("rounds=%d  capacity=%d  requests=%d  dup=%.0f%%  rps=%d\n\n",
		*gateRounds, *capacity, *gateReqs, *dupPct, *gateRPS)

	allOk := true
	for round := 1; round <= *gateRounds; round++ {
		if *gateRounds > 1 {
			fmt.Printf("── round %d/%d ──────────────────────────────────────────────────\n", round, *gateRounds)
		}
		allOk = gateRound(pool, reg, round) && allOk
	}

	if *gateRounds > 1 {
		fmt.Printf("── gate summary: %s ──────────────────────────────────────────────\n",
			map[bool]string{true: "ALL PASS", false: "SOME FAILED"}[allOk])
	}
	fmt.Println()
	return allOk
}

// calcDupN returns the dup count: --dup-n if set, else --dup-pct% of total.
func calcDupN(total int) int {
	if *dupN_flag > 0 {
		return *dupN_flag
	}
	return int(float64(total)**dupPct/100 + 0.5)
}

func gateRound(pool *redisPool, reg *lt.Reg, round int) bool {
	oppID := fmt.Sprintf("lt-gate-%d-%d", round, time.Now().UnixMilli())
	ck, mk := claimedKey(oppID), metaKey(oppID)
	now := time.Now().Unix()
	if err := setupOpp(pool, ck, mk, int64(*capacity), now, now+7200); err != nil {
		fmt.Printf("✗ setup failed: %v\n\n", err)
		return false
	}

	dupN := calcDupN(*gateReqs)
	// seedN drivers are fired first so they're guaranteed accepted before dups.
	// Sequence: seed(seedN) → dup(dupN) → rest(restN)
	// Expected: accepted=cap, dup=dupN, full=restN-(cap-seedN)
	seedN := dupN
	restN := *gateReqs - seedN - dupN

	seed := make([]driver, seedN)
	for i := range seed {
		seed[i].id, seed[i].iemKey = lt.NewDriver()
	}
	dups := make([]driver, dupN)
	for i := range dups {
		dups[i] = seed[i%seedN]
	}
	rest := make([]driver, restN)
	for i := range rest {
		rest[i].id, rest[i].iemKey = lt.NewDriver()
	}

	scenario := fmt.Sprintf("gate_r%d", round)
	var tal tally
	var lh latHist

	fireAll(pool, ck, mk, seed, *gateRPS, &tal, &lh, reg, scenario) // all accepted
	fireAll(pool, ck, mk, dups, *gateRPS, &tal, &lh, reg, scenario) // all DUP
	fireAll(pool, ck, mk, rest, *gateRPS, &tal, &lh, reg, scenario) // (cap-seedN) accepted, rest FULL

	p50, p99, _, _ := lh.stats()
	fmt.Printf("latency  p50=%s  p99=%s\n", p50.Round(time.Microsecond), p99.Round(time.Microsecond))

	scard, err := pool.scard(ck)
	if err != nil {
		fmt.Printf("✗ SCARD error: %v\n", err)
		return false
	}

	la := tal.accepted.Load()
	lf := tal.full.Load()
	ld := tal.dup.Load()

	wantFull := int64(restN - (*capacity - seedN))
	fmt.Printf("%-35s  %8s  %8s\n", "", "got", "want")
	fmt.Printf("%-35s  %8d  %8d\n", "accepted", la, int64(*capacity))
	fmt.Printf("%-35s  %8d  %8d\n", "full", lf, wantFull)
	fmt.Printf("%-35s  %8d  %8d\n", "dup", ld, int64(dupN))
	fmt.Printf("%-35s  %8d\n", "SCARD (ground truth)", scard)
	fmt.Println()

	ok := true
	ok = check("no oversell: accepted == capacity", la == int64(*capacity)) && ok
	ok = check("full == restN - (cap-seedN)", lf == wantFull) && ok
	ok = check("dup == dupN", ld == int64(dupN)) && ok
	ok = check("no errors", tal.errCount.Load() == 0) && ok
	ok = check("no lost responses: total == sent", tal.total() == int64(*gateReqs)) && ok
	ok = check("SCARD == local accepted  (Redis ↔ observation)", scard == la) && ok
	fmt.Println()
	return ok
}

// stressTest fires all requests completely concurrently (no rate limit) to
// maximize Redis contention and verify Lua atomicity under extreme load.
// Uses --gate-requests drivers against cap=--capacity, expects no oversell.
func stressTest(pool *redisPool, reg *lt.Reg) bool {
	fmt.Println("══ STRESS TEST ══════════════════════════════════════════════════════")
	fmt.Printf("firing %d requests ALL CONCURRENT (no rate limit), capacity=%d\n\n",
		*gateReqs, *capacity)

	oppID := fmt.Sprintf("lt-stress-%d", time.Now().UnixMilli())
	ck, mk := claimedKey(oppID), metaKey(oppID)
	now := time.Now().Unix()
	if err := setupOpp(pool, ck, mk, int64(*capacity), now, now+7200); err != nil {
		fmt.Printf("✗ setup failed: %v\n\n", err)
		return false
	}

	drivers := make([]driver, *gateReqs)
	for i := range drivers {
		drivers[i].id, drivers[i].iemKey = lt.NewDriver()
	}

	var tal tally
	var lh latHist
	start := time.Now()

	// Fire all concurrently — no ticker, no rate limit.
	var wg sync.WaitGroup
	for _, d := range drivers {
		wg.Add(1)
		d := d
		go func() {
			defer wg.Done()
			t := time.Now()
			c := pool.get()
			result, err := c.eval(luaScript, ck, mk, d.id)
			pool.put(c)
			lh.record(time.Since(t))
			if err != nil {
				tal.add("error")
				reg.Inc("stress", errorLabel(err))
				return
			}
			lbl := labelOf(result)
			tal.add(lbl)
			reg.Inc("stress", lbl)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	scard, err := pool.scard(ck)
	if err != nil {
		fmt.Printf("✗ SCARD error: %v\n\n", err)
		return false
	}

	p50, p99, _, max := lh.stats()
	la := tal.accepted.Load()

	fmt.Printf("elapsed=%s  actual_rps=%d\n", elapsed.Round(time.Millisecond), int(float64(*gateReqs)/elapsed.Seconds()))
	fmt.Printf("latency  p50=%s  p99=%s  max=%s\n\n", p50.Round(time.Microsecond), p99.Round(time.Microsecond), max.Round(time.Microsecond))
	fmt.Printf("accepted=%d  full=%d  errors=%d  SCARD=%d\n\n",
		la, tal.full.Load(), tal.errCount.Load(), scard)

	ok := true
	ok = check("no oversell: accepted == capacity", la == int64(*capacity)) && ok
	ok = check("SCARD == accepted  (Lua atomicity under full concurrency)", scard == la) && ok
	ok = check("no errors", tal.errCount.Load() == 0) && ok
	ok = check("no lost responses: total == sent", tal.total() == int64(*gateReqs)) && ok

	fmt.Println()
	return ok
}

// ── CLOSED CORRECTNESS TEST ───────────────────────────────────────────────────

func closedTest(pool *redisPool, reg *lt.Reg) bool {
	fmt.Println("══ CLOSED TEST ══════════════════════════════════════════════════════")

	oppID := fmt.Sprintf("lt-closed-%d", time.Now().UnixMilli())
	ck, mk := claimedKey(oppID), metaKey(oppID)
	now := time.Now().Unix()
	// window_start = now+2h → gate always sees now < window_start → CLOSED.
	if err := setupOpp(pool, ck, mk, int64(*capacity), now+7200, now+14400); err != nil {
		fmt.Printf("✗ setup failed: %v\n\n", err)
		return false
	}

	drivers := make([]driver, *closedN)
	for i := range drivers {
		drivers[i].id, drivers[i].iemKey = lt.NewDriver()
	}

	fmt.Printf("firing %d requests (window not open) at %d RPS...\n", *closedN, *gateRPS)

	var tal tally
	var lats latHist
	fireAll(pool, ck, mk, drivers, *gateRPS, &tal, &lats, reg, "closed")

	scard, _ := pool.scard(ck)
	promClosed, _ := scrapeCounter(*metricsAddr, "closed", "closed")
	promAccepted, _ := scrapeCounter(*metricsAddr, "closed", "accepted")

	fmt.Printf("closed=%d  accepted=%d  errors=%d  SCARD=%d\n",
		tal.closed.Load(), tal.accepted.Load(), tal.errCount.Load(), scard)
	fmt.Printf("Prometheus: closed=%d  accepted=%d\n\n", promClosed, promAccepted)

	ok := true
	ok = check("all closed", tal.closed.Load() == int64(*closedN)) && ok
	ok = check("accepted == 0 (no slip-through)", tal.accepted.Load() == 0) && ok
	ok = check("SCARD == 0 (nothing in claimed_set)", scard == 0) && ok
	ok = check("no errors", tal.errCount.Load() == 0) && ok
	ok = check("Prometheus closed == sent", promClosed == int64(*closedN)) && ok
	ok = check("Prometheus accepted == 0", promAccepted == 0) && ok

	fmt.Println()
	return ok
}

// ── Fire helpers ──────────────────────────────────────────────────────────────

type driver struct{ id, iemKey string }

// fireAll sends all drivers at the given RPS using a fixed worker pool.
// Uses a work channel so memory stays bounded regardless of len(drivers).
func fireAll(pool *redisPool, ck, mk string, drivers []driver, rps int,
	tal *tally, lats *latHist, reg *lt.Reg, scenario string) {

	const batchMs = 10
	batchSize := rps * batchMs / 1000
	if batchSize < 1 {
		batchSize = 1
	}
	work := make(chan driver, batchSize*4)

	// Fixed worker pool — avoids spawning O(len(drivers)) goroutines.
	nw := numWorkers()
	var wg sync.WaitGroup
	for i := 0; i < nw; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range work {
				t := time.Now()
				c := pool.get()
				result, err := c.eval(luaScript, ck, mk, d.id)
				pool.put(c)
				lats.record(time.Since(t))
				if err != nil {
					tal.add("error")
					reg.Inc(scenario, errorLabel(err))
					return
				}
				lbl := labelOf(result)
				tal.add(lbl)
				reg.Inc(scenario, lbl)
			}
		}()
	}

	// Rate-limited producer: batch every 10ms.
	ticker := time.NewTicker(batchMs * time.Millisecond)
	i := 0
	for i < len(drivers) {
		<-ticker.C
		end := i + batchSize
		if end > len(drivers) {
			end = len(drivers)
		}
		for _, d := range drivers[i:end] {
			work <- d
		}
		i = end
	}
	ticker.Stop()
	close(work)
	wg.Wait()
}

func labelOf(s string) string {
	switch s {
	case "OK":
		return "accepted"
	case "FULL":
		return "full"
	case "DUP":
		return "dup"
	case "CLOSED":
		return "closed"
	default:
		return "error"
	}
}

// ── Key helpers ───────────────────────────────────────────────────────────────

func claimedKey(oppID string) string { return "claimed_set:" + oppID }
func metaKey(oppID string) string    { return "opp_meta:" + oppID }

// ── Setup ─────────────────────────────────────────────────────────────────────

func setupOpp(pool *redisPool, ck, mk string, cap, winStart, winEnd int64) error {
	c := pool.get()
	defer pool.put(c)
	cmds := [][]string{
		{"DEL", ck},
		{"HSET", mk,
			"capacity", strconv.FormatInt(cap, 10),
			"window_start", strconv.FormatInt(winStart, 10)},
		{"EXPIREAT", mk, strconv.FormatInt(winEnd, 10)},
	}
	for _, cmd := range cmds {
		if _, err := c.doVal(cmd...); err != nil {
			return fmt.Errorf("%s: %w", cmd[0], err)
		}
	}
	return nil
}

func (p *redisPool) scard(key string) (int64, error) {
	c := p.get()
	defer p.put(c)
	val, err := c.doVal("SCARD", key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(val, 10, 64)
}

// ── Prometheus scraper ────────────────────────────────────────────────────────

// scrapeCounter fetches /metrics and extracts the counter value for the given
// (scenario, result) label pair.  Returns 0 (no error) if not yet recorded.
func scrapeCounter(addr, scenario, result string) (int64, error) {
	url := "http://localhost" + addr + "/metrics"
	if !strings.HasPrefix(addr, ":") {
		url = "http://" + addr + "/metrics"
	}
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	want := fmt.Sprintf(`{scenario=%q,result=%q}`, scenario, result)
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "loadtest_") {
			continue
		}
		if strings.Contains(line, want) {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				return strconv.ParseInt(parts[len(parts)-1], 10, 64)
			}
		}
	}
	return 0, nil
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

// stats returns p50, p99, p999, max.  Thread-safe.
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

// ── Minimal RESP connection pool ──────────────────────────────────────────────

type redisPool struct{ conns chan *redisConn }

type redisConn struct {
	net.Conn
	rd *bufio.Reader
	wr *bufio.Writer
}

func newPool(addr string, size int) (*redisPool, error) {
	p := &redisPool{conns: make(chan *redisConn, size)}
	for i := 0; i < size; i++ {
		c, err := dial(addr)
		if err != nil {
			return nil, err
		}
		p.conns <- c
	}
	return p, nil
}

func (p *redisPool) get() *redisConn { return <-p.conns }
func (p *redisPool) put(c *redisConn) { p.conns <- c }

func dial(addr string) (*redisConn, error) {
	raw, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &redisConn{
		Conn: raw,
		rd:   bufio.NewReader(raw),
		wr:   bufio.NewWriter(raw),
	}, nil
}

// doVal sends a RESP command and returns the scalar response string.
func (c *redisConn) doVal(args ...string) (string, error) {
	c.writeCmd(args...)
	if err := c.wr.Flush(); err != nil {
		return "", err
	}
	return c.readScalar()
}

// eval runs EVAL script 2 key1 key2 arg and returns the result string.
func (c *redisConn) eval(script, key1, key2, arg string) (string, error) {
	c.writeCmd("EVAL", script, "2", key1, key2, arg)
	if err := c.wr.Flush(); err != nil {
		return "", err
	}
	return c.readScalar()
}

func (c *redisConn) writeCmd(args ...string) {
	fmt.Fprintf(c.wr, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(c.wr, "$%d\r\n%s\r\n", len(a), a)
	}
}

func (c *redisConn) readScalar() (string, error) {
	line, err := c.rd.ReadString('\n')
	if err != nil {
		return "", err
	}
	s := strings.TrimRight(line, "\r\n")
	if len(s) == 0 {
		return "", errors.New("empty RESP line")
	}
	switch s[0] {
	case '+':
		return s[1:], nil
	case '-':
		return "", errors.New(s[1:])
	case ':':
		return s[1:], nil
	case '$':
		n, _ := strconv.Atoi(s[1:])
		if n < 0 {
			return "", nil
		}
		buf := make([]byte, n+2) // +2 for trailing \r\n
		if _, err := io.ReadFull(c.rd, buf); err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	default:
		return s, nil
	}
}
