package result

import "testing"

func TestFromStatus(t *testing.T) {
	cases := map[int]Outcome{
		200: Accepted,
		202: Accepted,
		409: Full,
		500: Unavail,
		0:   Unavail,
	}
	for code, want := range cases {
		if got := FromStatus(code); got != want {
			t.Errorf("FromStatus(%d) = %q, want %q", code, got, want)
		}
	}
}

func TestTallyAdd(t *testing.T) {
	var tally Tally
	tally.Add(Accepted)
	tally.Add(Accepted)
	tally.Add(Full)
	tally.Add(Unavail)
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
