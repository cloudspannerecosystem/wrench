// Copyright (c) 2020 Mercari, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package spanner

import (
	"context"
	"errors"
)

type cancellationSourceKey struct{}

// WithCancellationSource returns a copy of ctx that records ctx as the source
// of explicit cancellation (e.g. a context canceled by SIGINT/SIGTERM).
//
// When Config.WaitLongRunning is enabled, waiting for a long-running operation
// is not bounded by the deadline of the context passed to the method. However,
// once a context derived with context.WithTimeout exceeds its deadline, a later
// cancellation of its parent can no longer be observed through it. Recording
// the source here lets the wait still be canceled in that case.
func WithCancellationSource(ctx context.Context) context.Context {
	return context.WithValue(ctx, cancellationSourceKey{}, ctx)
}

// withoutDeadline returns a context that inherits values and explicit
// cancellation from parent but not its deadline. The returned context is
// canceled when parent, or the cancellation source recorded by
// WithCancellationSource, is canceled with context.Canceled (e.g. by a signal),
// but it is not canceled when parent exceeds its deadline.
//
// The returned CancelFunc must be called to release resources.
func withoutDeadline(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))

	stops := []func() bool{cancelOnCanceled(parent, cancel)}
	if src, ok := parent.Value(cancellationSourceKey{}).(context.Context); ok {
		stops = append(stops, cancelOnCanceled(src, cancel))
	}

	return ctx, func() {
		for _, stop := range stops {
			stop()
		}
		cancel()
	}
}

// cancelOnCanceled calls cancel when src is canceled with context.Canceled.
func cancelOnCanceled(src context.Context, cancel context.CancelFunc) func() bool {
	return context.AfterFunc(src, func() {
		if errors.Is(src.Err(), context.Canceled) {
			cancel()
		}
	})
}
