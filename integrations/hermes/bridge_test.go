// Package hermes holds the Hermes agent integration (SKILL.md + scripts/). The
// Go file only exists so `go test ./...` runs the bridge's Python tests.
package hermes

import (
	"os"
	"os/exec"
	"testing"
)

// TestBridgePython runs the stdlib unittest suite for scripts/thefeed_bridge.py
// against a fake client API. Skipped when python3 isn't installed.
func TestBridgePython(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not installed")
	}
	cmd := exec.Command(py, "-W", "error::ResourceWarning", "-m", "unittest", "discover", "-s", "tests")
	cmd.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1") // no __pycache__ in the tree
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bridge tests failed:\n%s", out)
	}
	t.Logf("%s", out)
}
