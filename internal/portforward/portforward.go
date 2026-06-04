/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package portforward wraps client-go's SPDY port-forwarder with the
// dial/ready/timeout handling shared by the kubectl-etcd plugin and the
// etcd-migrate tool. Both CLIs reach in-cluster etcd Pods from the
// operator's machine over `kubectl port-forward`-style tunnels.
package portforward

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// readyTimeout bounds how long ForwardToPod waits for the forward to signal
// ready before giving up.
const readyTimeout = 10 * time.Second

// ForwardToPod forwards a random local port to targetPort on the named Pod.
// It returns the local port and a stop function tearing the forward down.
// The forward stays up until stop is called (or the process exits).
func ForwardToPod(cfg *rest.Config, namespace, podName string, targetPort int) (uint16, func(), error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", namespace, podName)

	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create round tripper: %w", err)
	}
	hostURL, err := url.Parse(cfg.Host)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to parse host URL: %w", err)
	}
	hostURL.Path = path

	stopChan, readyChan := make(chan struct{}, 1), make(chan struct{}, 1)
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", hostURL)

	forwarder, err := portforward.New(dialer,
		[]string{fmt.Sprintf("0:%d", targetPort)}, stopChan, readyChan, &silentWriter{}, os.Stderr)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create port forwarder: %w", err)
	}

	// ForwardPorts blocks until the forward is torn down; run it in the
	// background and surface a startup failure via forwardErr. On a dial
	// failure (RBAC on pods/portforward, API-server connectivity, protocol
	// negotiation) ForwardPorts returns WITHOUT ever closing readyChan, so
	// blocking on readyChan alone would hang forever — awaitForward selects
	// on the error and a timeout too.
	forwardErr := make(chan error, 1)
	go func() {
		forwardErr <- forwarder.ForwardPorts()
	}()

	if err := awaitForward(readyChan, forwardErr, stopChan, readyTimeout); err != nil {
		return 0, nil, err
	}

	ports, err := forwarder.GetPorts()
	if err != nil {
		close(stopChan)
		return 0, nil, fmt.Errorf("failed to get forwarded ports: %w", err)
	}
	stop := func() { close(stopChan) }
	return ports[0].Local, stop, nil
}

// awaitForward blocks until the port-forward signals ready, fails, or times
// out — whichever comes first. It exists so a forward that dies before
// becoming ready (which leaves readyChan unclosed) surfaces as an error
// instead of hanging the CLI. On timeout it closes stopChan to tear the
// forwarder down.
func awaitForward(readyChan <-chan struct{}, forwardErr <-chan error, stopChan chan struct{}, timeout time.Duration) error {
	select {
	case <-readyChan:
		return nil
	case err := <-forwardErr:
		if err == nil {
			err = fmt.Errorf("exited before becoming ready")
		}
		return fmt.Errorf("port forwarding failed: %w", err)
	case <-time.After(timeout):
		close(stopChan)
		return fmt.Errorf("timed out after %s waiting for port forwarding to become ready", timeout)
	}
}

// silentWriter discards the forwarder's stdout chatter ("Forwarding from
// ...") so CLI output stays clean; errors still go to stderr.
type silentWriter struct{}

func (sw *silentWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
