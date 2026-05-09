package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// printJSON marshals v as indented JSON to stdout. Used by the
// --json output mode of the read-only inspection subcommands so
// ops automation can parse with jq instead of fragile text scrape.
func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("json encode: %w", err)
	}
	return nil
}
