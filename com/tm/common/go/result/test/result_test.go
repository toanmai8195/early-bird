package result_test

import (
	"testing"

	"github.com/tm/common/go/result"
)

func TestFromStatus(t *testing.T) {
	cases := map[int]result.Outcome{
		200: result.Accepted,
		202: result.Accepted,
		409: result.Full,
		500: result.Unavail,
		0:   result.Unavail,
	}
	for code, want := range cases {
		if got := result.FromStatus(code); got != want {
			t.Errorf("FromStatus(%d) = %q, want %q", code, got, want)
		}
	}
}

func TestTallyAdd(t *testing.T) {
	var tally result.Tally
	tally.Add(result.Accepted)
	tally.Add(result.Accepted)
	tally.Add(result.Full)
	tally.Add(result.Unavail)
	tally.Add("garbage") // unknown -> Unavail bucket

	if tally.Accepted != 2 {
		t.Errorf("Accepted = %d, want 2", tally.Accepted)
	}
	if tally.Full != 1 {
		t.Errorf("Full = %d, want 1", tally.Full)
	}
	if tally.Unavail != 2 {
		t.Errorf("Unavail = %d, want 2", tally.Unavail)
	}
}
