package serialization_checker

import (
	"fmt"
	"go/types"
	"log"
	"reflect"

	"golang.org/x/tools/go/packages"
)

// getTypeNameFromReflectType converts a reflect.Type to a *types.TypeName
func getTypeNameFromReflectType(t reflect.Type) *types.TypeName {
	// Load the package that contains the type using golang.org/x/tools/go/packages
	cfg := &packages.Config{
		Mode: packages.LoadAllSyntax,
	}

	// Load packages in the current module
	pkgs, err := packages.Load(cfg, t.PkgPath())
	if err != nil {
		log.Fatalf("failed to load packages: %v", err)
	}

	for _, pkg := range pkgs {
		if pkg.Types == nil || pkg.Types.Scope() == nil {
			continue
		}

		// Look for the type in the package scope
		for _, def := range pkg.Types.Scope().Names() {
			obj := pkg.Types.Scope().Lookup(def)
			if obj == nil {
				continue
			}

			// We want to find a type with the same name as the struct
			typeName, ok := obj.(*types.TypeName)
			if !ok {
				continue
			}

			if typeName.Name() == t.Name() {
				return typeName
			}
		}
	}

	return nil
}

func hasOnlyFixedSizedFields(typeName *types.TypeName) (bool, string) {
	fieldType := typeName.Type()

	switch fieldType := fieldType.(type) {
	case *types.Basic:
		kind := fieldType.Kind()
		if kind == types.Bool || kind == types.Byte || kind == types.Int || kind == types.Int8 || kind == types.Int16 || kind == types.Int32 || kind == types.Int64 ||
			kind == types.Uint || kind == types.Uint8 || kind == types.Uint16 || kind == types.Uint32 || kind == types.Uint64 ||
			kind == types.Float32 || kind == types.Float64 || kind == types.Complex64 || kind == types.Complex128 {
			// Fixed-sized types, continue checking the next field
			// fmt.Printf("basic field %v is fixed size\n", typeName.Name())

			return true, ""
		} else {
			return false, typeName.Name()
		}

	case *types.Named:
		underlyingType := fieldType.Underlying()
		// nestedStruct := fieldType
		underlyingTypeName := types.NewTypeName(0, nil, typeName.Name(), underlyingType)
		valid, culprit := hasOnlyFixedSizedFields(underlyingTypeName)
		if !valid {
			// fmt.Printf("named type field %v (underlying: %s) is not fixed size\n",
			// 	typeName.Name(), underlyingTypeName.String())
			return false, culprit
		}

	case *types.Pointer:
		// If it's a pointer, analyze the underlying type it points to
		elem := fieldType.Elem()
		if structType, ok := elem.Underlying().(*types.Struct); ok {
			nestedTypeName := types.NewTypeName(0, nil, typeName.Name(), structType)
			valid, culprit := hasOnlyFixedSizedFields(nestedTypeName)
			if !valid {
				fmt.Printf("pointer field %v is not fixed size\n", typeName.Name())
				return false, culprit
			}
		} else {
			// If the pointer doesn't point to a struct, it's considered non-fixed-size
			fmt.Printf("pointer field is not struct, and %v is not fixed size\n", typeName.Name())

			return false, typeName.Name()
		}

	case *types.Struct:
		// Recursively check if the nested struct has only fixed-size fields
		structType := typeName.Type().Underlying().(*types.Struct)
		for i := 0; i < structType.NumFields(); i++ {
			field := structType.Field(i)
			fieldType := field.Type()
			nestedTypeName := types.NewTypeName(0, nil, field.Name(), fieldType)
			valid, culprit := hasOnlyFixedSizedFields(nestedTypeName)
			if !valid {
				fmt.Printf("struct field %v is not fixed size\n", field.Name())
				return false, culprit
			}
		}
		return true, ""

	default:
		// Any other type is considered non-fixed-size
		fmt.Printf("got into default case, fieldName is %s, and fieldType is %+v\n",
			typeName.Name(), fieldType)
		return false, typeName.Name()
	}

	return true, ""
}
