package db

import (
	"regexp"
	"testing"
)

func TestMigrationNumbersAreUnique(t *testing.T) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	prefix := regexp.MustCompile(`^(\d{4})_`)
	seen := make(map[string]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		matches := prefix.FindStringSubmatch(entry.Name())
		if len(matches) != 2 {
			t.Errorf("migration %q must start with a four-digit number and underscore", entry.Name())
			continue
		}
		if previous, ok := seen[matches[1]]; ok {
			t.Errorf("migrations %q and %q share number %s", previous, entry.Name(), matches[1])
		}
		seen[matches[1]] = entry.Name()
	}
}
