// Command pg measures the raw throughput and correctness of the PgClaimStore
// bulk-settle CTE in isolation — no HTTP server, no Redis gate, no Kafka.
//
// It connects to Postgres directly, inserts opportunities, then runs the same
// bulk-settle SQL as PgClaimStore concurrently and records outcomes.
//
// # Scenario "contended"  (every 1 s)
//
// 20 opportunities are pre-created once at startup.  Each tick:
//   - Build 50 driver claims per opp (48 unique + 2 dup, ≈5 %).
//   - Run one settle CTE per opp in parallel (one goroutine / opp).
//
// This mirrors how the manager bulk-settles a Kafka poll batch: one statement
// per opportunity, opps in parallel, batch-50 per opp (see CLAUDE.md).
//
// # Scenario "diverse"  (every 1 s)
//
// 1 000 opportunities are pre-created once at startup.  Each tick:
//   - 1 driver claim per opp (5 % probability of being a dup).
//   - All settle CTEs run in parallel.
//
// Low contention per opp; tests PG throughput across many independent rows.
//
// Metrics: loadtest_pg_claims_total{scenario, result}
//
//	result ∈ {committed, duplicate, rejected, error, setup_error}
//
// Usage:
//
//	bazel run //com/tm/loadtest/pg -- \
//	  --pg=postgres://earlybird:earlybird@localhost:5432/earlybird?sslmode=disable \
//	  --metrics-addr=:9411
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"

	"github.com/tm/loadtest/lt"
)

// ── Global driver ID sequence ─────────────────────────────────────────────────

var driverSeq atomic.Int64

func newDriver() (id, iemKey string) {
	n := driverSeq.Add(1)
	id = fmt.Sprintf("dp%d", n) // "dp" = direct-pg, avoids collision with lt.NewDriver
	return id, id + "k"
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	pgDSN := flag.String("pg", "postgres://earlybird:earlybird@localhost:5432/earlybird?sslmode=disable", "Postgres DSN")
	metricsAddr := flag.String("metrics-addr", ":9411", "Prometheus /metrics listen address")
	contendedOpps := flag.Int("contended-opps", 20, "opps for contended scenario")
	contendedPerOpp := flag.Int("contended-per-opp", 50, "drivers per opp per tick (contended)")
	diverseOpps := flag.Int("diverse-opps", 1_000, "opps for diverse scenario")
	dupPct := flag.Float64("dup-pct", 5.0, "percent of requests that are dup re-sends (0–100)")
	// Large capacity: opps must not fill during a multi-hour run.
	capacity := flag.Int("capacity", 10_000_000, "opportunity capacity")
	region := flag.String("region", "lt", "region_id")
	zone := flag.String("zone", "lt", "zone_id")
	flag.Parse()

	db, err := sql.Open("postgres", *pgDSN)
	if err != nil {
		log.Fatalf("open PG: %v", err)
	}
	defer db.Close()
	// Pool: worst case = contended (20 opps settle in parallel) + diverse (1K in parallel).
	db.SetMaxOpenConns(*contendedOpps + *diverseOpps + 50)
	db.SetMaxIdleConns(*contendedOpps + *diverseOpps + 50)
	if err := db.Ping(); err != nil {
		log.Fatalf("ping PG: %v", err)
	}
	log.Printf("PG connected: %s", *pgDSN)

	reg := lt.NewReg(
		"loadtest_pg_claims_total",
		"PG bulk-settle CTE outcomes by scenario and result",
	)

	// Metrics HTTP server.
	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, reg.Text())
	})
	go func() {
		log.Printf("metrics on %s/metrics", *metricsAddr)
		if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
			log.Fatalf("metrics server: %v", err)
		}
	}()

	// All opps: window = [now-1s, now+4h] — open immediately.
	now := time.Now()
	winStart := now.Add(-1 * time.Second)
	winEnd := now.Add(4 * time.Hour)

	log.Printf("creating %d contended opps...", *contendedOpps)
	cOpps, err := setupOpps(db, "lt-c", *contendedOpps, *capacity, *region, *zone, winStart, winEnd)
	if err != nil {
		log.Fatalf("setup contended opps: %v", err)
	}
	log.Printf("creating %d diverse opps...", *diverseOpps)
	dOpps, err := setupOpps(db, "lt-d", *diverseOpps, *capacity, *region, *zone, winStart, winEnd)
	if err != nil {
		log.Fatalf("setup diverse opps: %v", err)
	}
	log.Printf("opps ready: %d contended + %d diverse", len(cOpps), len(dOpps))

	dupFrac := *dupPct / 100.0
	perOpp := *contendedPerOpp

	// contended: 20 opps × 50 drivers every second.
	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for range tick.C {
			go tickContended(db, cOpps, perOpp, dupFrac, reg)
		}
	}()

	// diverse: 1K opps × 1 driver every second.
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for range tick.C {
		go tickDiverse(db, dOpps, dupFrac, reg)
	}
}

// ── Opp management ────────────────────────────────────────────────────────────

// oppState holds the opportunity ID and a bounded pool of previously settled
// drivers so each tick can generate dup re-sends.
type oppState struct {
	id   string
	mu   sync.Mutex
	sent []driverPair // ring buffer (newest at tail)
}

type driverPair struct{ id, iemKey string }

const maxSent = 500

// nextDrivers returns count (id, iemKey) pairs:
// floor(count*(1-dupFrac)) fresh unique pairs + ceiling(count*dupFrac) dups.
func (o *oppState) nextDrivers(count int, dupFrac float64) []driverPair {
	dupN := int(float64(count)*dupFrac + 0.5)
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.sent) == 0 {
		dupN = 0 // no history on first tick
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

// nextDriver returns 1 pair: with dupProb it re-sends a past driver, else fresh.
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

// setupOpps inserts n opportunities directly into PG (50-concurrent).
func setupOpps(db *sql.DB, prefix string, n, capacity int, region, zone string, winStart, winEnd time.Time) ([]*oppState, error) {
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
				o.id, region, zone, winStart, winEnd, capacity,
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

// tickContended fires one settle CTE per opp concurrently.
func tickContended(db *sql.DB, opps []*oppState, perOpp int, dupFrac float64, reg *lt.Reg) {
	var wg sync.WaitGroup
	for _, o := range opps {
		wg.Add(1)
		o := o
		go func() {
			defer wg.Done()
			drivers := o.nextDrivers(perOpp, dupFrac)
			settle(db, o.id, drivers, reg, "contended")
		}()
	}
	wg.Wait()
}

// tickDiverse fires one settle CTE per opp (1 driver each) concurrently.
func tickDiverse(db *sql.DB, opps []*oppState, dupFrac float64, reg *lt.Reg) {
	var wg sync.WaitGroup
	for _, o := range opps {
		wg.Add(1)
		o := o
		go func() {
			defer wg.Done()
			d := o.nextDriver(dupFrac)
			settle(db, o.id, []driverPair{d}, reg, "diverse")
		}()
	}
	wg.Wait()
}

// ── Bulk-settle CTE (mirrors PgClaimStore.settleOpportunity) ─────────────────

// settle runs the same bulk-settle CTE as PgClaimStore for one opportunity.
// Outcomes (COMMITTED / DUPLICATE / REJECTED) are recorded in reg.
func settle(db *sql.DB, oppID string, drivers []driverPair, reg *lt.Reg, scenario string) {
	if len(drivers) == 0 {
		return
	}

	// Deduplicate by driver ID (preserve first occurrence, same as LinkedHashMap).
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
		reg.Inc(scenario, strings.ToLower(outcome)) // committed / duplicate / rejected
	}
	if err := rows.Err(); err != nil {
		reg.Inc(scenario, "error")
	}
}

// buildSettleSQL generates the bulk-settle CTE and its argument slice.
// The SQL is identical to PgClaimStore.buildSql(), adapted for lib/pq ($N) params.
func buildSettleSQL(oppID string, drivers []driverPair) (string, []any) {
	n := len(drivers)
	// Args: n*(driver_id, iemKey) then oppID × 4.
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
	base := n*2 + 1 // first oppID param index
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
