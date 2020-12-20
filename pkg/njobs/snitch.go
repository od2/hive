package njobs

// Snitch is supposed to report on task timeouts eventually.
//
// For now it just does nothing.
type Snitch struct {}

// Run consumes task expiration events with no-op.
func (s *Snitch) Run(exps <-chan []Expiration) {
	for range exps {}
}
