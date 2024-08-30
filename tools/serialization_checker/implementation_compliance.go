package serialization_checker

import (
	"fmt"
	"go/types"
	"log"

	"golang.org/x/tools/go/packages"
)

// ValidateStructsImplementingInterface scans the codebase to find structs implementing the specified interface
func ValidateStructsImplementingInterface(interfaceName string, pkgPattern string) []string {
	cfg := &packages.Config{
		Mode: packages.LoadAllSyntax,
	}
	pkgs, err := packages.Load(cfg, pkgPattern)
	if err != nil {
		log.Fatalf("failed to load packages: %v", err)
	}

	var structs []string
	for _, pkg := range pkgs {
		if pkg.Types == nil || pkg.Types.Scope() == nil {
			continue
		}

		// Find the interface type in the package scope
		interfaceType := findInterfaceByName(interfaceName, pkg.Types)
		if interfaceType == nil {
			continue
		}

		for _, def := range pkg.TypesInfo.Defs {
			if def == nil {
				continue
			}

			typeName, ok := def.(*types.TypeName)
			if !ok {
				continue
			}

			// Ensure the type's underlying kind is a struct before asserting
			if _, ok := typeName.Type().Underlying().(*types.Struct); !ok {
				continue
			}

			// log.Printf("checking for type %+v implementing %+v\n", typeName, interfaceType)

			// Check if the struct implements the specified interface
			implements := types.Implements(typeName.Type(), interfaceType)
			if !implements {
				// check pointer type
				ptrType := types.NewPointer(typeName.Type())
				if !types.Implements(ptrType, interfaceType) {
					continue
				}
				fmt.Printf("the pointer type[%+v] matches the interface\n", ptrType)
			}

			structs = append(structs, typeName.Name())

			valid, culpritField := hasOnlyFixedSizedFields(typeName)
			if !valid {
				log.Fatalf("Field %s in struct %s is not of fixed size type", culpritField, typeName.Name())
			}

			fmt.Printf("Struct %s implements %s and complies with fixed size constraints.\n", typeName.Name(), interfaceName)
		}
	}

	return structs
}

// findInterfaceByName looks up an interface by name in a package's scope
func findInterfaceByName(interfaceName string, pkg *types.Package) *types.Interface {
	scope := pkg.Scope()
	for _, name := range scope.Names() {
		if name == interfaceName {
			if iface, ok := scope.Lookup(name).Type().Underlying().(*types.Interface); ok {
				return iface
			}
		}
	}
	return nil
}
