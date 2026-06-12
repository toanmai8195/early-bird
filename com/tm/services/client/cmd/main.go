// Command client is a load-generating / simulation driver client. It fires
// concurrent claim requests at the server to exercise the early-reject gate and
// the end-to-end booking flow, tallying outcomes via common/go/result.
package main

import (
	"flag"
	"log"

	"github.com/tm/common/go/result"
)

func main() {
	var (
		target  = flag.String("target", "http://localhost:8080", "server base URL")
		oppID   = flag.String("opportunity", "opp-1", "opportunity_id to claim")
		drivers = flag.Int("drivers", 10000, "number of concurrent drivers")
	)
	flag.Parse()

	log.Printf("firing %d concurrent claims at %s for %s", *drivers, *target, *oppID)

	var tally result.Tally
	// TODO: spawn N goroutines, each POSTs a claim with a unique driver_id +
	// idempotency_key, then: tally.Add(result.FromStatus(resp.StatusCode)).
	log.Printf("results: accepted=%d full=%d unavailable=%d",
		tally.Accepted, tally.Full, tally.Unavail)
}
