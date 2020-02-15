package avro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestPrimitiveSchema(t *testing.T) {
	primitiveSchemaAssert(t, "string", String, "STRING")
	primitiveSchemaAssert(t, "int", Int, "INT")
	primitiveSchemaAssert(t, "long", Long, "LONG")
	primitiveSchemaAssert(t, "boolean", Boolean, "BOOLEAN")
	primitiveSchemaAssert(t, "float", Float, "FLOAT")
	primitiveSchemaAssert(t, "double", Double, "DOUBLE")
	primitiveSchemaAssert(t, "bytes", Bytes, "BYTES")
	primitiveSchemaAssert(t, "null", Null, "NULL")
}

func primitiveSchemaAssert(t *testing.T, raw string, expected int, typeName string) {
	s, err := ParseSchema(raw)
	assert(t, err, nil)

	if s.Type() != expected {
		t.Errorf("\n%s \n===\n Should parse into Type() = %s", raw, typeName)
	}
}

func TestArraySchema(t *testing.T) {
	//array of strings
	raw := `{"type":"array", "items": "string"}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Array {
		t.Errorf("\n%s \n===\n Should parse into Type() = %s", raw, "ARRAY")
	}
	if s.(*ArraySchema).Items.Type() != String {
		t.Errorf("\n%s \n===\n Array item type should be STRING", raw)
	}

	//array of longs
	raw = `{"type":"array", "items": "long"}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Array {
		t.Errorf("\n%s \n===\n Should parse into Type() = %s", raw, "ARRAY")
	}
	if s.(*ArraySchema).Items.Type() != Long {
		t.Errorf("\n%s \n===\n Array item type should be LONG", raw)
	}

	//array of arrays of strings
	raw = `{"type":"array", "items": {"type":"array", "items": "string"}}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Array {
		t.Errorf("\n%s \n===\n Should parse into Type() = %s", raw, "ARRAY")
	}
	if s.(*ArraySchema).Items.Type() != Array {
		t.Errorf("\n%s \n===\n Array item type should be ARRAY", raw)
	}
	if s.(*ArraySchema).Items.(*ArraySchema).Items.Type() != String {
		t.Errorf("\n%s \n===\n Array's nested item type should be STRING", raw)
	}

	raw = `{"type":"array", "items": {"type": "record", "name": "TestRecord", "fields": [
	{"name": "longRecordField", "type": "long"},
	{"name": "floatRecordField", "type": "float"}
	]}}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Array {
		t.Errorf("\n%s \n===\n Should parse into Type() = %s", raw, "ARRAY")
	}
	if s.(*ArraySchema).Items.Type() != Record {
		t.Errorf("\n%s \n===\n Array item type should be RECORD", raw)
	}
	if s.(*ArraySchema).Items.(*RecordSchema).Fields[0].Type.Type() != Long {
		t.Errorf("\n%s \n===\n Array's nested record first field type should be LONG", raw)
	}
	if s.(*ArraySchema).Items.(*RecordSchema).Fields[1].Type.Type() != Float {
		t.Errorf("\n%s \n===\n Array's nested record first field type should be FLOAT", raw)
	}
}

func TestMapSchema(t *testing.T) {
	//map[string, int]
	raw := `{"type":"map", "values": "int"}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Map {
		t.Errorf("\n%s \n===\n Should parse into MapSchema. Actual %#v", raw, s)
	}
	if s.(*MapSchema).Values.Type() != Int {
		t.Errorf("\n%s \n===\n Map value type should be Int. Actual %#v", raw, s.(*MapSchema).Values)
	}

	//map[string, []string]
	raw = `{"type":"map", "values": {"type":"array", "items": "string"}}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Map {
		t.Errorf("\n%s \n===\n Should parse into MapSchema. Actual %#v", raw, s)
	}
	if s.(*MapSchema).Values.Type() != Array {
		t.Errorf("\n%s \n===\n Map value type should be Array. Actual %#v", raw, s.(*MapSchema).Values)
	}
	if s.(*MapSchema).Values.(*ArraySchema).Items.Type() != String {
		t.Errorf("\n%s \n===\n Map nested array item type should be String. Actual %#v", raw, s.(*MapSchema).Values.(*ArraySchema).Items)
	}

	//map[string, [int, string]]
	raw = `{"type":"map", "values": ["int", "string"]}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Map {
		t.Errorf("\n%s \n===\n Should parse into MapSchema. Actual %#v", raw, s)
	}
	if s.(*MapSchema).Values.Type() != Union {
		t.Errorf("\n%s \n===\n Map value type should be Union. Actual %#v", raw, s.(*MapSchema).Values)
	}
	if s.(*MapSchema).Values.(*UnionSchema).Types[0].Type() != Int {
		t.Errorf("\n%s \n===\n Map nested union's first type should be Int. Actual %#v", raw, s.(*MapSchema).Values.(*UnionSchema).Types[0])
	}
	if s.(*MapSchema).Values.(*UnionSchema).Types[1].Type() != String {
		t.Errorf("\n%s \n===\n Map nested union's second type should be String. Actual %#v", raw, s.(*MapSchema).Values.(*UnionSchema).Types[1])
	}

	//map[string, record]
	raw = `{"type":"map", "values": {"type": "record", "name": "TestRecord2", "fields": [
	{"name": "doubleRecordField", "type": "double"},
	{"name": "fixedRecordField", "type": {"type": "fixed", "size": 4, "name": "bytez"}}
	]}}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Map {
		t.Errorf("\n%s \n===\n Should parse into MapSchema. Actual %#v", raw, s)
	}
	if s.(*MapSchema).Values.Type() != Record {
		t.Errorf("\n%s \n===\n Map value type should be Record. Actual %#v", raw, s.(*MapSchema).Values)
	}
	if s.(*MapSchema).Values.(*RecordSchema).Fields[0].Type.Type() != Double {
		t.Errorf("\n%s \n===\n Map value's record first field should be Double. Actual %#v", raw, s.(*MapSchema).Values.(*RecordSchema).Fields[0].Type)
	}
	if s.(*MapSchema).Values.(*RecordSchema).Fields[1].Type.Type() != Fixed {
		t.Errorf("\n%s \n===\n Map value's record first field should be Fixed. Actual %#v", raw, s.(*MapSchema).Values.(*RecordSchema).Fields[1].Type)
	}
}

func TestRecordSchema(t *testing.T) {
	raw := `{"type": "record", "name": "TestRecord", "fields": [
     	{"name": "longRecordField", "type": "long"},
     	{"name": "stringRecordField", "type": "string"},
     	{"name": "intRecordField", "type": "int"},
     	{"name": "floatRecordField", "type": "float"}
     ]}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Record {
		t.Errorf("\n%s \n===\n Should parse into RecordSchema. Actual %#v", raw, s)
	}
	if s.(*RecordSchema).Fields[0].Type.Type() != Long {
		t.Errorf("\n%s \n===\n Record's first field type should parse into LongSchema. Actual %#v", raw, s.(*RecordSchema).Fields[0].Type)
	}
	if s.(*RecordSchema).Fields[1].Type.Type() != String {
		t.Errorf("\n%s \n===\n Record's second field type should parse into StringSchema. Actual %#v", raw, s.(*RecordSchema).Fields[1].Type)
	}
	if s.(*RecordSchema).Fields[2].Type.Type() != Int {
		t.Errorf("\n%s \n===\n Record's third field type should parse into IntSchema. Actual %#v", raw, s.(*RecordSchema).Fields[2].Type)
	}
	if s.(*RecordSchema).Fields[3].Type.Type() != Float {
		t.Errorf("\n%s \n===\n Record's fourth field type should parse into FloatSchema. Actual %#v", raw, s.(*RecordSchema).Fields[3].Type)
	}

	raw = `{"namespace": "scalago",
	"type": "record",
	"name": "PingPong",
	"fields": [
	{"name": "counter", "type": "long"},
	{"name": "name", "type": "string"}
	]}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Record {
		t.Errorf("\n%s \n===\n Should parse into RecordSchema. Actual %#v", raw, s)
	}
	if s.(*RecordSchema).Name != "PingPong" {
		t.Errorf("\n%s \n===\n Record's name should be PingPong. Actual %#v", raw, s.(*RecordSchema).Name)
	}
	f0 := s.(*RecordSchema).Fields[0]
	if f0.Name != "counter" {
		t.Errorf("\n%s \n===\n Record's first field name should be 'counter'. Actual %#v", raw, f0.Name)
	}
	if f0.Type.Type() != Long {
		t.Errorf("\n%s \n===\n Record's first field type should parse into LongSchema. Actual %#v", raw, f0.Type)
	}
	f1 := s.(*RecordSchema).Fields[1]
	if f1.Name != "name" {
		t.Errorf("\n%s \n===\n Record's first field name should be 'counter'. Actual %#v", raw, f0.Name)
	}
	if f1.Type.Type() != String {
		t.Errorf("\n%s \n===\n Record's second field type should parse into StringSchema. Actual %#v", raw, f1.Type)
	}
}

func TestEnumSchema(t *testing.T) {
	raw := `{"type":"enum", "name":"foo", "symbols":["A", "B", "C", "D"]}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Enum {
		t.Errorf("\n%s \n===\n Should parse into EnumSchema. Actual %#v", raw, s)
	}
	if s.(*EnumSchema).Name != "foo" {
		t.Errorf("\n%s \n===\n Enum name should be 'foo'. Actual %#v", raw, s.(*EnumSchema).Name)
	}
	if !arrayEqual(s.(*EnumSchema).Symbols, []string{"A", "B", "C", "D"}) {
		t.Errorf("\n%s \n===\n Enum symbols should be [\"A\", \"B\", \"C\", \"D\"]. Actual %#v", raw, s.(*EnumSchema).Symbols)
	}
}

func TestUnionSchema(t *testing.T) {
	raw := `["null", "string"]`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Union {
		t.Errorf("\n%s \n===\n Should parse into UnionSchema. Actual %#v", raw, s)
	}
	if s.(*UnionSchema).Types[0].Type() != Null {
		t.Errorf("\n%s \n===\n Union's first type should be Null. Actual %#v", raw, s.(*UnionSchema).Types[0])
	}
	if s.(*UnionSchema).Types[1].Type() != String {
		t.Errorf("\n%s \n===\n Union's second type should be String. Actual %#v", raw, s.(*UnionSchema).Types[1])
	}

	raw = `["string", "null"]`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Union {
		t.Errorf("\n%s \n===\n Should parse into UnionSchema. Actual %#v", raw, s)
	}
	if s.(*UnionSchema).Types[0].Type() != String {
		t.Errorf("\n%s \n===\n Union's first type should be String. Actual %#v", raw, s.(*UnionSchema).Types[0])
	}
	if s.(*UnionSchema).Types[1].Type() != Null {
		t.Errorf("\n%s \n===\n Union's second type should be Null. Actual %#v", raw, s.(*UnionSchema).Types[1])
	}
}

func TestFixedSchema(t *testing.T) {
	raw := `{"type": "fixed", "size": 16, "name": "md5"}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Fixed {
		t.Errorf("\n%s \n===\n Should parse into FixedSchema. Actual %#v", raw, s)
	}
	if s.(*FixedSchema).Size != 16 {
		t.Errorf("\n%s \n===\n Fixed size should be 16. Actual %#v", raw, s.(*FixedSchema).Size)
	}
	if s.(*FixedSchema).Name != "md5" {
		t.Errorf("\n%s \n===\n Fixed name should be md5. Actual %#v", raw, s.(*FixedSchema).Name)
	}
}

func TestSchemaRegistryMap(t *testing.T) {
	rawSchema1 := `{"type": "record", "name": "TestRecord", "namespace": "com.github.elodina", "fields": [
		{"name": "longRecordField", "type": "long"}
	]}`

	rawSchema2 := `{"type": "record", "name": "TestRecord2", "namespace": "com.github.elodina", "fields": [
		{"name": "record", "type": ["null", "TestRecord"]}
	]}`

	rawSchema3 := `{"type": "record", "name": "TestRecord3", "namespace": "com.github.other", "fields": [
		{"name": "record", "type": ["null", "com.github.elodina.TestRecord2"]}
	]}`

	rawSchema4 := `{"type": "record", "name": "TestRecord3", "namespace": "com.github.elodina", "fields": [
		{"name": "record", "type": ["null", {"type": "TestRecord2"}, "com.github.other.TestRecord3"]}
	]}`

	registry := make(map[string]Schema)

	s1, err := ParseSchemaWithRegistry(rawSchema1, registry)
	assert(t, err, nil)
	assert(t, s1.Type(), Record)
	assert(t, len(registry), 1)

	s2, err := ParseSchemaWithRegistry(rawSchema2, registry)
	assert(t, err, nil)
	assert(t, s2.Type(), Record)
	assert(t, len(registry), 2)

	s3, err := ParseSchemaWithRegistry(rawSchema3, registry)
	assert(t, err, nil)
	assert(t, s3.Type(), Record)
	assert(t, len(registry), 3)

	s4, err := ParseSchemaWithRegistry(rawSchema4, registry)
	assert(t, err, nil)
	assert(t, s4.Type(), Record)
	assert(t, len(registry), 4)
}

func TestRecordCustomProps(t *testing.T) {
	raw := `{"type": "record", "name": "TestRecord", "hello": "world", "fields": [
     	{"name": "longRecordField", "type": "long"},
     	{"name": "stringRecordField", "type": "string"},
     	{"name": "intRecordField", "type": "int"},
     	{"name": "floatRecordField", "type": "float"}
     ]}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	assert(t, len(s.(*RecordSchema).Properties), 1)

	value, exists := s.Prop("hello")
	assert(t, exists, true)
	assert(t, value, "world")
}

func TestLoadSchemas(t *testing.T) {
	schemas := LoadSchemas("test/schemas/")
	assert(t, len(schemas), 4)

	_, exists := schemas["example.avro.Complex"]
	assert(t, exists, true)
	_, exists = schemas["example.avro.foo"]
	assert(t, exists, true)
}

func TestSchemaEquality(t *testing.T) {

	s0, _ := ParseSchema(`{"type": "record", "name": "TestRecord", "namespace": "xyz", "hello": "world", "fields": [
		{"name": "field1", "type": "long"},
		{"name": "field2", "type": "string", "doc": "hello world"}
	]}`)

	s1, _ := ParseSchema(`{"type": "record", "name": "TestRecord", "namespace": "xyz", "hello": "world", "fields": [
		{"name": "field1", "type": "long"},
		{"name": "field2", "type": "string", "doc": "hello"}
	]}`)
	s2, _ := ParseSchema(`{"type": "record", "name": "TestRecord", "hello": "world", "fields": [
		{"name": "field1", "type": "long", "aliases": ["f1"] },
		{"name": "field2", "type": "string", "doc": "hello"}
	]}`)

	s_enum1, _ := ParseSchema(`{"type":"enum", "name":"foo", "symbols":["A", "B", "C", "D"], "doc": "hello"}`)
	s_enum2, _ := ParseSchema(`{"type":"enum", "name":"foo", "symbols":["D", "C", "B", "A"]}`)
	s_fixed1, _ := ParseSchema(`{"type": "fixed", "size": 16, "name": "md5"}`)
	s_fixed2, _ := ParseSchema(`{"type": "fixed", "size": 32, "name": "md5"}`)
	s_fixedSame, _ := ParseSchema(`{"type": "fixed", "size": 16, "name": "md5", "doc": "xyz"}`)
	f1, _ := s_fixed1.Fingerprint()
	f2, _ := s_fixedSame.Fingerprint()
	assert(t, f1, f2)
	s_array1, _ := ParseSchema(`{"type":"array", "items": "string"}`)
	s_array2, _ := ParseSchema(`{"type":"array", "items": "long"}`)
	s_map1, _ := ParseSchema(`{"type":"map", "values": "float"}`)
	s_map2, _ := ParseSchema(`{"type":"map", "values": "double"}`)
	s_union1, _ := ParseSchema(`["null", "string"]`)
	s_union2, _ := ParseSchema(`["string", "null"]`)
	s_union3, _ := ParseSchema(`["string", "int", "float"]`)

	f3, _ := s0.Fingerprint()
	f4, _ := s1.Fingerprint()
	assert(t, f3, f4)

	normal, _ := json.Marshal(s_enum1)
	assert(t, string(normal), `{"type":"enum","name":"foo","doc":"hello","symbols":["A","B","C","D"]}`)
	canonical, _ := s_enum1.Canonical()
	c, _ := canonical.MarshalJSON()
	//doc is stripped from canonical
	assert(t, string(c), `{"name":"foo","type":"enum","symbols":["A","B","C","D"]}`)

	schemas := []Schema{
		s1, s2,
		s_enum1, s_enum2,
		s_fixed1, s_fixed2,
		s_array1, s_array2,
		s_map1, s_map2,
		s_union1, s_union2, s_union3,
		new(StringSchema),
		new(BytesSchema),
		new(IntSchema),
		new(LongSchema),
		new(FloatSchema),
		new(DoubleSchema),
		new(BooleanSchema),
		new(NullSchema),
	}
	for i := range schemas {
		for y := range schemas {
			f1, _ := schemas[i].Fingerprint()
			f2, _ := schemas[y].Fingerprint()
			if y == i {
				assert(t, f1.Equal(f2), true)
			} else if f1.Equal(f2) {
				if !reflect.DeepEqual(schemas[i], schemas[y]) {
					panic(fmt.Errorf("different schemas have same fingerprint: \n%q\n%q",
						schemas[i], schemas[y]))
				}
			}
		}
	}

	af1, _ := s1.Fingerprint()
	af1_, _ := newRecursiveSchema(s1.(*RecordSchema)).Fingerprint()
	assert(t, af1.Equal(af1_), true)
	af2, _ := s2.Fingerprint()
	af2_, _ := newRecursiveSchema(s2.(*RecordSchema)).Fingerprint()
	assert(t, af2.Equal(af2_), true)

}

func TestBenchmark(t *testing.T) {
	s1, _ := ParseSchema(`{"type": "record", "name": "TestRecord", "namespace": "xyz", "hello": "world", "fields": [
		{"name": "field1", "type": "long"},
		{"name": "field2", "type": "string", "doc": "hello"}
	]}`)
	s2, _ := ParseSchema(`{"type": "record", "name": "TestRecord", "hello": "world", "fields": [
		{"name": "field1", "type": "long", "aliases": ["f1"] },
		{"name": "field2", "type": "string", "doc": "hello"}
	]}`)
	s3, _ := ParseSchema(`{"type": "record", "name": "TestRecord", "hello": "world", "fields": [
		{"name": "field1", "type": "long", "aliases": ["f1"] },{"name": "field2", "doc": "hello", "type": "string"}
	]}`)

	f1, _ := calculateSchemaFingerprint(s1)
	f2, _ := calculateSchemaFingerprint(s2)
	f3, _ := calculateSchemaFingerprint(s3)

	result1 := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if reflect.DeepEqual(s1, s2) {
				panic("1")
			}
			if !reflect.DeepEqual(s2, s3) {
				panic("2")
			}
		}
	})

	result2 := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if f1.Equal(f2) {
				panic("1")
			}
			if !f2.Equal(f3) {
				panic(fmt.Errorf("2 \n%v\n%v\n", f2, f3))
			}
		}
	})

	//assert that matching on fingerprint is no more than a few nanos
	assert(t, result2.NsPerOp() < 100, true)
	//assert that using fingerprints is 100s times faster then deep equal on the actual schema
	assert(t, result1.NsPerOp()/result2.NsPerOp() > 100, true)
}

func TestCanonicalConstituentOrdering(t *testing.T) {
	var Schema17 = `{"type":"record","namespace":"domain","name":"Instr","fields":[
	{"name": "zindex", "type": "int"},
	{"name":"operation","type":[
		{"type":"record","name":"MODIFY","fields":[{"name":"key","type":"string"},{"name": "value","type":"string"}]},
		{"type":"record","name":"REPLACE","fields":[{"name":"key","type":"string"},{"name": "data","type":"bytes"}]},
		{"type":"record","name":"DELETE","fields":[{"name":"key","type":"string"},{"name": "cascading","type":"boolean"}]}
	]}]}`
	var Schema18 = `{"type":"record","namespace":"domain","name":"Instr","fields":[
	{"name": "zindex", "type": "int"},
	{"name":"operation","type":[
		{"type":"record","name":"MODIFY","fields":[{"name":"key","type":"string"},{"name": "value","type":"string"}]},
		{"type":"record","name":"REPLACE","fields":[{"name":"key","type":"string"},{"name": "data","type":"bytes"}]},
		{"type":"record","name":"DELETE","fields":[{"name":"key","type":"string"},{"name": "cascading","type":"boolean"}]}
	]}]}`
	var Schema19 = `{"type":"record","namespace":"domain","name":"Instr","fields":[
	{"name":"operation","type":[
		{"type":"record","name":"DELETE","fields":[{"name":"key","type":"string"},{"name": "cascading","type":"boolean"}]},
		{"type":"record","name":"MODIFY","fields":[{"name":"key","type":"string"},{"name": "value","type":"string"}]},
		{"type":"record","name":"REPLACE","fields":[{"name":"key","type":"string"},{"name": "data","type":"bytes"}]}
	]},
	{"name": "zindex", "type": "int"}
]}`
	var Schema20 = `{"type":"record","namespace":"domain","name":"Instr","fields":[
	{"name":"operation","type":[
		{"type":"record","name":"DELETE","fields":[{"name":"key","type":"string"},{"name": "cascading","type":"boolean"}]},
		{"type":"record","name":"MODIFY","fields":[{"name":"key","type":"string"},{"name": "value","type":"string"}]},
		{"type":"record","name":"REPLACE","fields":[{"name":"key","type":"string"},{"name": "data","type":"bytes"}]}
	]},
	{"name": "zindex", "type": "int"}
]}`

	s17 := MustParseSchema(Schema17)
	s18 := MustParseSchema(Schema18)
	s19 := MustParseSchema(Schema19)
	s20 := MustParseSchema(Schema20)
	f17, _ := s17.Fingerprint()
	f18, _ := s18.Fingerprint()
	assert(t, f17.Equal(f18), true) //nested record field order doesn't matter
	f19, _ := s19.Fingerprint()
	f20, _ := s20.Fingerprint()
	assert(t, f18.Equal(f19), false) //union types order matters
	assert(t, f19.Equal(f20), true)  //record field order doesn't matter

}

func TestSchemaConvertGeneric(t *testing.T) {
	schema := MustParseSchema(`{
	    "type": "record",
	    "name": "Rec",
	    "fields": [
	        {
	            "name": "dict",
	            "type": {
					"type": "map", 
					"values": { "type": "array", "items": "string" }
				}
	        }, {
				"name": "select",
				"type": { "type": "enum", "name": "something", "symbols": [ "A", "B", "C"] },
				"default": "B"
			}, {
				"name": "option",
				"type": [ "null", { "type": "enum", "name": "something", "symbols": [ "A", "B", "C"] }],
				"default": null
			}, {
				"name": "option2",
				"type": [ "null", { "type": "map", "values": "long" }],
				"default": null
			}, {
				"name": "option3",
				"type": [ "null", { "type": "enum", "name": "something", "symbols": [ "A", "B", "C"] }],
				"default": "A"
			}
	    ]
	}`)

	type Datum struct {
		Dict    map[string][]string
		Select  EnumValue
		Option  *EnumValue
		Option2 *map[string]uint64
		Option3 *EnumValue
	}

	datum := map[string]interface{}{
		"dict": map[interface{}]interface{}{
			"A1": []interface{}{"abc", "def"},
			"G1": []interface{}{"ghi", "jkl"},
		},
		"option": "C",
	}

	generic, err := schema.Generic(datum)
	if err != nil {
		panic(err)
	}
	rec, ok := generic.(*GenericRecord)
	if !ok {
		panic("not a record")
	}
	assert(t, rec.String(), `{"dict":{"A1":["abc","def"],"G1":["ghi","jkl"]},"option":"C","option2":null,"option3":"A","select":"B"}`)

	datum2 := new(Datum)
	if err := json.Unmarshal([]byte(rec.String()), &datum2); err != nil {
		panic(err)
	}
	buffer2 := new(bytes.Buffer)
	if err := NewDatumWriter(schema).Write(datum2, NewBinaryEncoder(buffer2)); err != nil {
		panic(err)
	}


	assert(t, datum2.Option.String(), "C")
	assert(t, datum2.Option2 == nil , true)
	assert(t, datum2.Select.String(), "B")
	assert(t, datum2.Option3.String(), "A")
	generic2, ok := generic.(*GenericRecord)
	if !ok {
		panic("not a record")
	}
	assert(t, generic, generic2)

	buffer := new(bytes.Buffer)
	if err := NewDatumWriter(schema).Write(generic2, NewBinaryEncoder(buffer)); err != nil {
		panic(err)
	}
	bytes := buffer.Bytes()

	specificDatum := new(Datum)
	if err := NewDatumReader(schema).Read(specificDatum, NewBinaryDecoder(bytes)); err != nil {
		panic(err)
	}
	assert(t, specificDatum.Dict, map[string][]string{
		"A1": {"abc", "def"},
		"G1": {"ghi", "jkl"},
	})
	assert(t, specificDatum.Option.String(), "C")
	assert(t, specificDatum.Option2 == nil, true)
	assert(t, specificDatum.Select.String(), "B")

	projectedDatum := new(Datum)
	projector, err := NewDatumProjector(schema, schema)
	if err != nil {
		panic(err)
	}
	if err := projector.Read(projectedDatum, NewBinaryDecoder(bytes)); err != nil {
		panic(err)
	}
	assert(t, projectedDatum.Dict, map[string][]string{
		"A1": {"abc", "def"},
		"G1": {"ghi", "jkl"},
	})
	assert(t, projectedDatum.Option.String(), "C")
	assert(t, projectedDatum.Option2 == nil, true)
	assert(t, projectedDatum.Select.String(), "B")

}

func TestCorrectParsingOfSchema(t *testing.T) {
	jsonSchema := `{
  "type" : "record",
  "name" : "Referenced",
  "namespace" : "io.avro",
  "fields" : [ {
    "name" : "A",
    "type" : {
      "type" : "enum",
      "name" : "Status",
      "symbols" : [ "OK", "FAILED" ]
    }
  }, {
    "name" : "B",
    "type" : "Status"
  }, {
    "name" : "C",
    "type" : {
      "type" : "map",
      "values" : "Status"
    }
  }, {
    "name" : "D",
    "type" : {
      "type" : "array",
      "items" : "Status"
    }
  }, {
    "name" : "E",
    "type" : [ "null", "Status" ]
  }, {
    "name" : "F",
    "type" : {
      "type" : "record",
	  "name": "F",
      "fields" : [ 
        {
            "name": "X",
            "type": "Status"
        }
      ]
    }
  }
	,
  {
    "name" : "G",
    "type" : {
      "type": "map",
	  "values": {
		"type" : "record",
	    "name": "F",
        "fields" : [ 
			{
				"name": "X",
				"type": "Status"
			}
        ]
      }
    }
  }


 ]
}`
	expectJson := `{"type":"record","namespace":"io.avro","name":"Referenced","fields":[{"name":"A","type":{"type":"enum","name":"Status","symbols":["OK","FAILED"]}},{"name":"B","type":"Status"},{"name":"C","type":{"type":"map","values":"Status"}},{"name":"D","type":{"type":"array","items":"Status"}},{"name":"E","default":null,"type":["null","Status"]},{"name":"F","type":{"type":"record","name":"F","fields":[{"name":"X","type":"Status"}]}},{"name":"G","type":{"type":"map","values":"F"}}]}`
	if schema, err := ParseSchema(jsonSchema); err != nil {
		panic(err)
	} else if json, err := json.Marshal(schema); err != nil {
		panic(err)
	} else {
		assert(t, string(json), expectJson)
	}

}

func arrayEqual(arr1 []string, arr2 []string) bool {
	if len(arr1) != len(arr2) {
		return false
	}

	for i := range arr1 {
		if arr1[i] != arr2[i] {
			return false
		}
	}
	return true
}
