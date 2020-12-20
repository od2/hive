package njobs

// Streamer accepts connections from a worker and pushes down assignments.
//
// Re-connecting and running multiple connections is also supported.
// When the worker is absent for too long, the streamer shuts down.
type Streamer struct {

}
