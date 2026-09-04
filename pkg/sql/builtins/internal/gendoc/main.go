// Command gendoc writes the Functions reference from the builtins
// registry: go run ./internal/gendoc <path>.
package main

import (
	"fmt"
	"os"

	"github.com/sthorne/datax/pkg/sql/builtins"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: gendoc <output.md>")
		os.Exit(2)
	}
	if err := os.WriteFile(os.Args[1], []byte(builtins.Reference()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
