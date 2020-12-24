package njobs

import (
	"context"
	"sort"

	"github.com/Shopify/sarama"
)

// FanOut forwards assignments from the assigner to streamers.
type FanOut struct {
}

// Run loops over incoming assignments and fans them out to streamers.
func (f *FanOut) Run(ctx context.Context, assignmentsC <-chan []Assignment) error {
loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-assignmentsC:
			if !ok {
				break loop
			}
			if err := f.step(ctx, batch); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FanOut) step(ctx context.Context, batch []Assignment) error {
	groups := groupAssignmentsByWorkerID(batch)

}

func groupAssignmentsByWorkerID(batch []Assignment) (groups []assignmentGroup) {
	if len(batch) <= 0 {
		return nil
	}
	// Sort the batch by workers.
	sort.Slice(batch, func(i, j int) bool {
		return batch[i].Worker < batch[j].Worker
	})
	// Group by workers.
	group := assignmentGroup{workerID: batch[0].Worker}
	for _, assign := range batch {
		if group.workerID != assign.Worker {
			groups = append(groups, group)
			group = assignmentGroup{workerID: assign.Worker}
		}
		group.msgs = append(group.msgs, assign.ConsumerMessage)
	}
	if len(group.msgs) > 0 {
		groups = append(groups, group)
	}
	return
}

type assignmentGroup struct {
	workerID int64
	msgs     []*sarama.ConsumerMessage
}
