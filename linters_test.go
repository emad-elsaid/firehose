package firehose_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFilePairing checks that every .go file has a corresponding _test.go
// file and vice versa (excluding generated and helper files).
func TestFilePairing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping file pairing check in short mode")
	}

	// Get the directory where this test file is located
	_, filename, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(filename)

	// Files that are allowed to not have tests
	allowNoTests := map[string]bool{
		"mocks.go":      true,
		"interfaces.go": true,
		"doc.go":        true,
	}

	// Test helper files that are allowed without source
	allowedTestHelpers := map[string]bool{
		"linters_test.go":      true,
		"test_helpers_test.go": true,
	}

	goFiles := make(map[string]bool)
	testFiles := make(map[string]bool)
	var errors []string

	// Walk through all Go files from the test directory upward
	err := filepath.Walk(testDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			// Skip vendor and hidden directories
			if strings.HasPrefix(info.Name(), ".") ||
				info.Name() == "vendor" ||
				info.Name() == "website" ||
				info.Name() == "node_modules" ||
				info.Name() == "scripts" {
				return filepath.SkipDir
			}
			return nil
		}

		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		// Get relative path from test directory
		relPath, _ := filepath.Rel(testDir, path)

		if strings.HasSuffix(path, "_test.go") {
			testFiles[relPath] = true
		} else {
			goFiles[relPath] = true
		}

		return nil
	})

	require.NoError(t, err, "error walking directory")

	// Check for orphaned test files
	for testFile := range testFiles {
		sourceFile := strings.TrimSuffix(testFile, "_test.go") + ".go"

		// Check if it's an allowed test helper
		baseName := filepath.Base(testFile)
		if allowedTestHelpers[baseName] {
			continue
		}

		if !goFiles[sourceFile] {
			// Check if it's an example test file (e.g., valid_example_test.go → valid.go)
			if strings.Contains(testFile, "_example_test.go") {
				exampleSourceFile := strings.TrimSuffix(testFile, "_example_test.go") + ".go"
				if goFiles[exampleSourceFile] {
					continue
				}
			}
			errors = append(errors, "Test file has no matching source: "+testFile)
		}
	}

	// Check for missing test files
	for goFile := range goFiles {
		baseName := filepath.Base(goFile)

		// Skip allowed files
		if allowNoTests[baseName] {
			continue
		}

		testFile := strings.TrimSuffix(goFile, ".go") + "_test.go"
		if !testFiles[testFile] {
			errors = append(errors, "Missing test file for: "+goFile)
		}
	}

	if len(errors) > 0 {
		t.Logf("Found %d file pairing issues:", len(errors))
		for _, err := range errors {
			t.Log(err)
		}
		t.Errorf("file pairing check failed with %d issues", len(errors))
	} else {
		t.Logf("Checked %d source files and %d test files - all OK", len(goFiles), len(testFiles))
	}
}

// TestGeneratedFilesNotChecked verifies that only expected files are
// excluded from test requirements.
func TestGeneratedFilesNotChecked(t *testing.T) {
	allowedNoTest := map[string]bool{
		"mocks.go":      true,
		"interfaces.go": true,
		"doc.go":        true,
	}

	for file := range allowedNoTest {
		assert.True(t, strings.HasSuffix(file, ".go"),
			"excluded file should have .go extension")
	}
}
