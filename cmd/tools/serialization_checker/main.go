package main

import (
	"fmt"

	"KeyValor/tools/serialization_checker"
)

func main() {
	// Replace "Header" with the interface name you want to check
	// Replace "./..." with the package pattern to check (e.g., "KeyValor/...")

	structs := serialization_checker.ValidateStructsImplementingInterface("Header", "./...")
	fmt.Printf("Found structs implementing 'Header', and they have fixed sized fields: %v\n", structs)
}
