package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"

	"kafka-pilot/internal/serve"
)

// startServe starts the local HTTP service.
func startServe(port int) error {
	if port == 0 {
		var err error
		port, err = findFreePort()
		if err != nil {
			return fmt.Errorf("find free port: %w", err)
		}
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	fmt.Printf("Kafka Pilot serving on http://%s\n", addr)

	server := serve.NewServer(flagApp)
	return server.ListenAndServe(addr)
}

// startDesktop starts serve and opens the browser.
func startDesktop() error {
	port, err := findFreePort()
	if err != nil {
		return fmt.Errorf("find free port: %w", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	url := fmt.Sprintf("http://%s", addr)

	go func() {
		if err := startServe(port); err != nil {
			fmt.Fprintf(os.Stderr, "serve error: %v\n", err)
		}
	}()

	// Wait for server to start
	_ = waitForServer(url)

	// Open browser
	openBrowser(url)

	// Block forever
	select {}
}

func findFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitForServer(url string) error {
	for i := 0; i < 50; i++ {
		resp, err := http.Get(url + "/api/health")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("server did not start")
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	if cmd != nil {
		cmd.Start()
	}
}
