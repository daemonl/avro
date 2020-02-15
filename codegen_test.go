package avro

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"
)

func TestGoStructFieldName(t *testing.T) {

	for input, expect := range map[string]string{
		"name":       "Name",
		"first_name": "FirstName",
		"first-name": "FirstName",
		"firstName":  "FirstName",
		"fieldID":    "FieldID",
		"field-id":   "FieldId",
	} {

		got := toGoStructFieldName(input)
		if got != expect {
			t.Errorf("Expected %s -> %s, got %s", input, expect, got)
		}
	}
}

type structField struct {
	GoName  string
	GoType  string
	AvroTag string
}

func extractStructTypes(code string) (map[string]*ast.StructType, error) {
	// Extract the struct definitions from the code
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", code, 0)
	if err != nil {
		return nil, err
	}

	// Print the AST. Useful when tests fail
	//ast.Print(fset, f)

	structs := map[string]*ast.StructType{}
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		if gd.Tok != token.TYPE {
			continue
		}
		typeSpec, ok := gd.Specs[0].(*ast.TypeSpec)
		if !ok {
			continue
		}

		structType, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			continue
		}

		structs[typeSpec.Name.Name] = structType
	}

	return structs, nil
}

func TestCodegen(t *testing.T) {

	for _, testCase := range []struct {
		name   string
		schema string
		expect map[string][]structField
	}{{
		name: "basic",
		schema: `{
			"type": "record",
			"name": "Person",
			"fields": [
				{"name": "first_name", "type": "string"},
				{"name": "age", "type": "int"}
			]
		}`,
		expect: map[string][]structField{
			"Person": {{
				GoName:  "FirstName",
				GoType:  "string",
				AvroTag: "first_name",
			}, {
				GoName:  "Age",
				GoType:  "int32",
				AvroTag: "age",
			}},
		},
	}, {
		name: "nested",
		schema: `{
			"type": "record",
			"name": "Person",
			"fields": [
				{
					"name": "address",
					"type": {
						"type": "record",
						"name": "address",
						"fields": [
							{ "name": "suburb", "type": "string" }
						]
					}
				}
			]
		}`,
		expect: map[string][]structField{
			"Address": {{
				GoName:  "Suburb",
				GoType:  "string",
				AvroTag: "suburb",
			}},
			"Person": {{
				GoName:  "Address",
				GoType:  "*Address",
				AvroTag: "address",
			}},
		},
	}, {
		name: "array",
		schema: `{
			"type": "record",
			"name": "Person",
			"fields": [{
				"name": "nameArray",
				"type": {
					"type": "array",
					"items": "string"
				}
			}]
		}`,
		expect: map[string][]structField{
			"Person": {{
				GoName:  "NameArray",
				GoType:  "[]string",
				AvroTag: "nameArray",
			}},
		},
	}, {
		name: "optional",
		schema: `{
			"type": "record",
			"name": "Person",
			"fields": [
				{"name": "first_name", "type": [ "null", "string" ]},
				{
					"name": "address",
					"type": [
						"null",
						{
							"type": "record",
							"name": "address",
							"fields": [
								{ "name": "suburb", "type": "string" }
							]
						}
					]
				}
			]
		}`,
		expect: map[string][]structField{
			"Person": {{
				GoName:  "FirstName",
				GoType:  "*string",
				AvroTag: "first_name",
			}, {
				GoName:  "Address",
				GoType:  "*Address",
				AvroTag: "address",
			}},
		},
	}} {
		t.Run(testCase.name, func(t *testing.T) {
			gen := NewCodeGenerator([]string{testCase.schema})
			generatedCode, err := gen.Generate()
			if err != nil {
				t.Fatal(err)
			}
			t.Log(generatedCode)

			structs, err := extractStructTypes(generatedCode)
			if err != nil {
				t.Fatal(err.Error())
			}

			for typeName, expectedFields := range testCase.expect {
				structDef, ok := structs[typeName]
				if !ok {
					t.Errorf("No type %s in generated code", typeName)
					return
				}

				for idx, expected := range expectedFields {
					if idx >= len(structDef.Fields.List) {
						t.Fatalf("Index %d (%s) out of range of fields", idx, expected.GoType)
					}

					field := structDef.Fields.List[idx]
					if len(field.Names) != 1 {
						t.Fatalf("%d name fields for %s", len(field.Names), expected.GoType)
					}
					if field.Names[0].Name != expected.GoName {
						t.Errorf("Bad field name, want %s got %s", expected.GoName, field.Names[0].Name)
					}

					typeName := rebuildTypeName(field.Type)

					if typeName != expected.GoType {
						t.Errorf("Bad Type for %s: %s", expected.GoName, typeName)
					}

					// AST version includes ``, reflect does not
					structTag := reflect.StructTag(field.Tag.Value[1 : len(field.Tag.Value)-1])
					avroTag := structTag.Get("avro")
					if avroTag != expected.AvroTag {
						t.Errorf("Bad struct tag: %s (%s)", field.Tag.Value, avroTag)
					}
				}
			}

		})
	}
}

func rebuildTypeName(fieldType ast.Expr) string {

	if _, ok := fieldType.(*ast.InterfaceType); ok {
		// TODO: Non empty interface?
		return "interface{}"
	}

	if arrayExpr, ok := fieldType.(*ast.ArrayType); ok {
		return "[]" + rebuildTypeName(arrayExpr.Elt)
	}

	if starExpr, ok := fieldType.(*ast.StarExpr); ok {
		return "*" + rebuildTypeName(starExpr.X)
	}

	typeIdent, ok := fieldType.(*ast.Ident)
	if !ok {
		return "UNKNOWN TYPE"
	}

	return typeIdent.Name
}
