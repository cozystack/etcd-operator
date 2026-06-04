/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package portforward

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// ── A failed/never-ready port-forward must not hang ─────────────────────────

func TestAwaitForward_Ready(t *testing.T) {
	ready := make(chan struct{}, 1)
	close(ready)
	if err := awaitForward(ready, make(chan error, 1), make(chan struct{}, 1), time.Second); err != nil {
		t.Fatalf("ready forward should succeed, got %v", err)
	}
}

func TestAwaitForward_ErrorBeforeReady(t *testing.T) {
	// ForwardPorts returns an error without ever closing readyChan — the old
	// code blocked on <-readyChan forever. awaitForward must return the error.
	forwardErr := make(chan error, 1)
	forwardErr <- fmt.Errorf("dial tcp: connection refused")
	err := awaitForward(make(chan struct{}), forwardErr, make(chan struct{}, 1), time.Second)
	if err == nil {
		t.Fatal("a forward failure must return an error, not hang or succeed")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error should wrap the forward failure, got %v", err)
	}
}

func TestAwaitForward_Timeout(t *testing.T) {
	stop := make(chan struct{}, 1)
	// Nothing ever signals ready and no error arrives → must time out, not hang.
	err := awaitForward(make(chan struct{}), make(chan error, 1), stop, 10*time.Millisecond)
	if err == nil {
		t.Fatal("a never-ready forward must time out, not hang")
	}
	select {
	case <-stop:
	default:
		t.Error("timeout must close stopChan to tear the forwarder down")
	}
}
