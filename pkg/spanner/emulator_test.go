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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	defaultEmulatorHost  = "localhost:9010"
	defaultEmulatorREST  = "http://localhost:9020"
	emulatorImage        = "gcr.io/cloud-spanner-emulator/emulator:1.5.58"
	emulatorStartupLimit = 30 * time.Second
)

type emulatorProcess struct {
	runtime     string
	name        string
	emulatorURL string
	restURL     string
}

func TestMain(m *testing.M) {
	process, err := startTestEmulator()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	code := m.Run()
	if process != nil {
		if err := process.stop(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to stop Spanner emulator: %v\n", err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}

func startTestEmulator() (*emulatorProcess, error) {
	project := os.Getenv(envSpannerProjectID)
	if project == "" {
		project = "wrench-test-project"
		if err := os.Setenv(envSpannerProjectID, project); err != nil {
			return nil, fmt.Errorf("set %s: %w", envSpannerProjectID, err)
		}
	}

	instance := os.Getenv(envSpannerInstanceID)
	if instance == "" {
		instance = "wrench-test-instance"
		if err := os.Setenv(envSpannerInstanceID, instance); err != nil {
			return nil, fmt.Errorf("set %s: %w", envSpannerInstanceID, err)
		}
	}

	host := os.Getenv(envSpannerEmulatorHost)
	if host != "" {
		if !emulatorAvailable(host) {
			return nil, fmt.Errorf("Spanner emulator at %q is unavailable", host)
		}
		if localEmulatorHost(host) {
			if err := ensureTestInstance(project, instance, defaultEmulatorREST); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}

	if emulatorAvailable(defaultEmulatorHost) {
		if err := os.Setenv(envSpannerEmulatorHost, defaultEmulatorHost); err != nil {
			return nil, fmt.Errorf("set %s: %w", envSpannerEmulatorHost, err)
		}
		if err := ensureTestInstance(project, instance, defaultEmulatorREST); err != nil {
			return nil, err
		}
		return nil, nil
	}

	process, err := launchEmulator()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), emulatorStartupLimit)
	defer cancel()
	if err := waitForEmulator(ctx, process.emulatorURL, process.restURL); err != nil {
		_ = process.stop()
		return nil, err
	}
	if err := os.Setenv(envSpannerEmulatorHost, process.emulatorURL); err != nil {
		_ = process.stop()
		return nil, fmt.Errorf("set %s: %w", envSpannerEmulatorHost, err)
	}
	if err := ensureTestInstance(project, instance, process.restURL); err != nil {
		_ = process.stop()
		return nil, err
	}
	return process, nil
}

func localEmulatorHost(host string) bool {
	return host == "localhost:9010" || host == "127.0.0.1:9010"
}

func emulatorAvailable(host string) bool {
	conn, err := net.DialTimeout("tcp", host, 250*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func launchEmulator() (*emulatorProcess, error) {
	var failures []string
	for _, runtime := range []string{"docker", "podman"} {
		if _, err := exec.LookPath(runtime); err != nil {
			continue
		}

		name := fmt.Sprintf("wrench-spanner-test-%d", os.Getpid())
		// Ask the container runtime for free host ports to avoid the usual emulator ports.
		args := []string{
			"run", "--detach", "--rm", "--name", name,
			"-p", "127.0.0.1::9010", "-p", "127.0.0.1::9020", emulatorImage,
		}
		if output, err := exec.Command(runtime, args...).CombinedOutput(); err == nil {
			process := &emulatorProcess{runtime: runtime, name: name}
			if err := process.discoverPorts(); err != nil {
				_ = process.stop()
				return nil, err
			}
			return process, nil
		} else {
			failures = append(failures, fmt.Sprintf("%s: %s", runtime, strings.TrimSpace(string(output))))
		}
	}

	if len(failures) == 0 {
		return nil, fmt.Errorf("Spanner emulator is unavailable and neither Docker nor Podman is installed")
	}
	return nil, fmt.Errorf("failed to start Spanner emulator: %s", strings.Join(failures, "; "))
}

func (p *emulatorProcess) discoverPorts() error {
	grpcPort, err := p.mappedPort("9010/tcp")
	if err != nil {
		return err
	}
	restPort, err := p.mappedPort("9020/tcp")
	if err != nil {
		return err
	}
	p.emulatorURL = net.JoinHostPort("127.0.0.1", grpcPort)
	p.restURL = "http://" + net.JoinHostPort("127.0.0.1", restPort)
	return nil
}

func (p *emulatorProcess) mappedPort(containerPort string) (string, error) {
	output, err := exec.Command(p.runtime, "port", p.name, containerPort).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s port %s %s: %s: %w", p.runtime, p.name, containerPort,
			strings.TrimSpace(string(output)), err)
	}
	for _, address := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		_, port, err := net.SplitHostPort(strings.TrimSpace(address))
		if err == nil {
			return port, nil
		}
	}
	return "", fmt.Errorf("%s returned no mapped host port for %s", p.runtime, containerPort)
}

func (p *emulatorProcess) stop() error {
	output, err := exec.Command(p.runtime, "stop", p.name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s stop %s: %s: %w", p.runtime, p.name, strings.TrimSpace(string(output)), err)
	}
	return nil
}

func waitForEmulator(ctx context.Context, host, restURL string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if emulatorAvailable(host) && emulatorRESTAvailable(restURL) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("Spanner emulator did not become available at %q: %w", host, ctx.Err())
		case <-ticker.C:
		}
	}
}

func emulatorRESTAvailable(restURL string) bool {
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(restURL)
	if err != nil {
		return false
	}
	_ = response.Body.Close()
	return true
}

func ensureTestInstance(project, instance, restURL string) error {
	instanceURL := fmt.Sprintf("%s/v1/projects/%s/instances/%s", restURL,
		url.PathEscape(project), url.PathEscape(instance))
	client := &http.Client{Timeout: 3 * time.Second}

	response, err := client.Get(instanceURL)
	if err != nil {
		return fmt.Errorf("check Spanner test instance: %w", err)
	}
	_ = response.Body.Close()
	if response.StatusCode == http.StatusOK {
		return nil
	}
	if response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("check Spanner test instance: unexpected HTTP status %s", response.Status)
	}

	body, err := json.Marshal(map[string]string{"instanceId": instance})
	if err != nil {
		return fmt.Errorf("encode Spanner test instance request: %w", err)
	}
	request, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/v1/projects/%s/instances", restURL, url.PathEscape(project)),
		bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create Spanner test instance request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err = client.Do(request)
	if err != nil {
		return fmt.Errorf("create Spanner test instance: %w", err)
	}
	if err := response.Body.Close(); err != nil {
		return fmt.Errorf("close Spanner test instance response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("create Spanner test instance: unexpected HTTP status %s", response.Status)
	}
	return nil
}
