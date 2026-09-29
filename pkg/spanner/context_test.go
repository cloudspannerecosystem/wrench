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
	"testing"
	"time"
)

type ctxKey struct{}

func TestWithoutDeadline(t *testing.T) {
	t.Parallel()

	t.Run("does not inherit deadline", func(t *testing.T) {
		t.Parallel()

		parent, cancel := context.WithTimeout(context.WithValue(context.Background(), ctxKey{}, "v"), 10*time.Millisecond)
		defer cancel()

		ctx, stop := withoutDeadline(parent)
		defer stop()

		if _, ok := ctx.Deadline(); ok {
			t.Fatal("ctx must not have deadline")
		}
		if got := ctx.Value(ctxKey{}); got != "v" {
			t.Fatalf("ctx must inherit values, got %v", got)
		}

		<-parent.Done()
		if !errors.Is(parent.Err(), context.DeadlineExceeded) {
			t.Fatalf("parent err: %v", parent.Err())
		}

		select {
		case <-ctx.Done():
			t.Fatalf("ctx must not be canceled by parent deadline: %v", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("inherits explicit cancellation", func(t *testing.T) {
		t.Parallel()

		parent, cancel := context.WithCancel(context.Background())
		ctx, stop := withoutDeadline(parent)
		defer stop()

		cancel()

		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf("ctx err: %v", ctx.Err())
			}
		case <-time.After(time.Second):
			t.Fatal("ctx must be canceled when parent is canceled")
		}
	})

	t.Run("inherits cancellation through a timeout context", func(t *testing.T) {
		t.Parallel()

		root, cancelRoot := context.WithCancel(context.Background())
		parent, cancel := context.WithTimeout(root, time.Hour)
		defer cancel()

		ctx, stop := withoutDeadline(parent)
		defer stop()

		cancelRoot()

		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf("ctx err: %v", ctx.Err())
			}
		case <-time.After(time.Second):
			t.Fatal("ctx must be canceled when root is canceled")
		}
	})
}
