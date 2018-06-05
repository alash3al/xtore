package xtore

// Result result data structure
type Result struct {
	Totals int64       `json:"totals"`
	Hits   []*Document `json:"hits"`
	Time   float64     `json:"time"`
}
