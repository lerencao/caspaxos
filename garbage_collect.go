package caspaxos

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// ErrStateUpdated indicates a key was re-written to a nonempty value before the
// garbage collection process could completely remove it. The new value has
// "won" and the client should take some corrective action, likely re-issuing a
// delete.
var ErrStateUpdated = errors.New("state updated before garbage collection could complete")

// GarbageCollect implements the garbage collection process outlined in section
// 3.1 "How to delete a record" in the paper. It will continue until the key is
// successfully garbage collected or the context is canceled.
func GarbageCollect(ctx context.Context, key string, delay time.Duration, logger log.Logger, proposers ...Proposer) error {
	// From the paper, this process: "(a) Replicates an empty value to all nodes
	// by executing the identity transform with 2F+1 quorum size. Reschedules
	// itself if at least one node is down."
	for {
		if err := gcBroadcast(ctx, key, proposers...); err == context.Canceled {
			return err
		} else if err != nil {
			level.Debug(logger).Log("broadcast", "failed", "err", err)
			select {
			case <-time.After(time.Second):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// From the paper: "(b) For each proposer, fast-forwards its counter to
		// generate ballot numbers greater than the tombstone's number."
		if err := gcFastForward(ctx, proposers...); err != nil {
			return err
		}

		// From the paper: "(c) Wait some time to make sure that all in-channel
		// messages with lesser ballots were delivered."
		time.Sleep(delay)

		// From the paper: "(d) [Each] acceptor [should] remove the register if
		// its value is empty."
		var err error
		for _, p := range proposers {
			// There is no a prioiri list of acceptors, that information is
			// known only to proposers. So we ask the first proposer to
			// broadcast the request to all of its known acceptors. If that
			// fails, we try the next proposer.
			err = p.RemoveIfEmpty(ctx, key)
			switch {
			case err == nil:
				return nil // success, great
			case err == context.Canceled:
				return err // fatal, give up
			case err != nil:
				continue // nonfatal, retry
			}
		}

		// All failed.
		return err
	}
}

func gcBroadcast(ctx context.Context, key string, proposers ...Proposer) error {
	// The identity read change function.
	identity := func(x []byte) []byte { return x }

	// Gather results in the results chan.
	type result struct {
		newState []byte
		err      error
	}
	results := make(chan result, len(proposers))

	// Broadcast to all proposers.
	for _, p := range proposers {
		go func(p Proposer) {
			newState, err := p.Propose(ctx, key, identity)
			results <- result{newState, err}
		}(p)
	}

	// Wait for every result.
	// Any error is a failure.
	for i := 0; i < cap(results); i++ {
		result := <-results
		if result.err != nil {
			return result.err
		}
		if len(result.newState) != 0 {
			return ErrStateUpdated
		}
	}

	return nil
}

func gcFastForward(ctx context.Context, proposers ...Proposer) error {
	// Collect results in the results chan.
	results := make(chan error, len(proposers))

	// Broadcast the fast-forward request.
	for _, p := range proposers {
		go func(p Proposer) {
			results <- p.FastForward()
		}(p)
	}

	// Verify results.
	for i := 0; i < cap(results); i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	// Good.
	return nil
}
