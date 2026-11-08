package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"kafka-pilot/internal/app"
	"kafka-pilot/internal/config"

	"github.com/spf13/cobra"
)

const pidFileName = "kp.pid"

// errProcessGone indicates the recorded PID is no longer running.
var errProcessGone = errors.New("process not running")

func pidFilePath() string {
	return filepath.Join(config.AppDataDir(), pidFileName)
}

// writePIDFile records the serve process PID and port so `kp stop` / `kp
// restart` can locate the running instance. It lives in the same config
// directory the serve process already resolves via KP_APPDATA / default.
func writePIDFile(port int) error {
	data, err := json.MarshalIndent(map[string]interface{}{
		"pid":        os.Getpid(),
		"port":       port,
		"started_at": time.Now().Format(time.RFC3339),
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(pidFilePath(), data, 0o644)
}

func readPIDFile() (pid int, port int, err error) {
	b, err := os.ReadFile(pidFilePath())
	if err != nil {
		return 0, 0, err
	}
	var v struct {
		PID  int `json:"pid"`
		Port int `json:"port"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return 0, 0, err
	}
	return v.PID, v.Port, nil
}

func removePIDFile() {
	_ = os.Remove(pidFilePath())
}

// killProcess terminates the given PID in a cross-platform way.
func killProcess(pid int) error {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("taskkill", "/PID", strconv.Itoa(pid), "/F")
	} else {
		cmd = exec.Command("kill", "-9", strconv.Itoa(pid))
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		s := string(out)
		if strings.Contains(s, "not found") || strings.Contains(s, "no such process") ||
			strings.Contains(s, "No such") {
			return errProcessGone
		}
		return fmt.Errorf("kill pid %d failed: %v (%s)", pid, err, s)
	}
	return nil
}

// stopServe stops the running service recorded in the pid file.
func stopServe() error {
	pid, _, err := readPIDFile()
	if err != nil {
		return fmt.Errorf("no running Kafka Pilot service found (missing %s)", pidFilePath())
	}
	if err := killProcess(pid); err != nil {
		if errors.Is(err, errProcessGone) {
			removePIDFile()
			return fmt.Errorf("service (PID %d) was not running; removed stale pid file", pid)
		}
		return err
	}
	removePIDFile()
	fmt.Printf("Stopped Kafka Pilot service (PID %d)\n", pid)
	return nil
}

// restartServe stops the old service (if any) and becomes the new serve
// process, reloading config from the same KP_APPDATA directory.
func restartServe() error {
	pid, port, err := readPIDFile()
	if err == nil {
		kerr := killProcess(pid)
		if kerr != nil && !errors.Is(kerr, errProcessGone) {
			return fmt.Errorf("failed to stop old service (PID %d): %w", pid, kerr)
		}
		if kerr == nil {
			fmt.Printf("Stopped old service (PID %d)\n", pid)
		} else {
			fmt.Printf("Old service (PID %d) was not running; starting fresh\n", pid)
		}
		removePIDFile()
	} else {
		fmt.Println("No running service found; starting fresh.")
	}

	a, aerr := app.New()
	if aerr != nil {
		return fmt.Errorf("init app: %w", aerr)
	}
	flagApp = a
	return startServe(port)
}

// newStopCmd stops the running service. It does not need a Kafka connection,
// so it overrides PersistentPreRunE to skip app initialization.
func newStopCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "stop",
		Short: "Stop the running Kafka Pilot service",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return stopServe()
		},
	}
	return c
}

// newRestartCmd restarts the service with the same port and reloaded config.
func newRestartCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "restart",
		Short: "Restart the Kafka Pilot service (same port, config reloaded)",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return restartServe()
		},
	}
	return c
}
