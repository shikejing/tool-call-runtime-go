package runtime

import "testing"

func f64(v float64) *float64 { return &v }
func i(v int) *int           { return &v }

func testSchema() *ParamSchema {
	return &ParamSchema{
		Type: TypeObject,
		Properties: map[string]*ParamSchema{
			"name": {Type: TypeString, Required: true, MinLen: i(1), MaxLen: i(8)},
			"age":  {Type: TypeInteger, Min: f64(0), Max: f64(150)},
			"role": {Type: TypeString, Enum: []any{"admin", "user"}},
			"addr": {
				Type: TypeObject,
				Properties: map[string]*ParamSchema{
					"city": {Type: TypeString, Required: true},
					"zip":  {Type: TypeString},
				},
			},
			"tags": {Type: TypeArray, Items: &ParamSchema{Type: TypeString}},
		},
	}
}

func TestValidateValid(t *testing.T) {
	args := map[string]any{
		"name": "bob",
		"age":  30,
		"role": "admin",
		"addr": map[string]any{"city": "sh", "zip": "200000"},
		"tags": []any{"a", "b"},
	}
	if r := Validate(testSchema(), args); r != nil {
		t.Fatalf("expect valid, got %v", r)
	}
}

func TestValidateRejections(t *testing.T) {
	cases := []struct {
		name string
		args map[string]any
		kind RejectKind
		path string
	}{
		{"missing required", map[string]any{}, KindMissingRequired, "$.name"},
		{"type mismatch", map[string]any{"name": 1.5}, KindTypeMismatch, "$.name"},
		{"unknown field", map[string]any{"name": "a", "hack": 1}, KindUnknownField, "$.hack"},
		{"range high", map[string]any{"name": "a", "age": 200}, KindOutOfRange, "$.age"},
		{"range low", map[string]any{"name": "a", "age": -1}, KindOutOfRange, "$.age"},
		{"not integer", map[string]any{"name": "a", "age": 1.5}, KindTypeMismatch, "$.age"},
		{"enum", map[string]any{"name": "a", "role": "root"}, KindEnumViolation, "$.role"},
		{"strlen", map[string]any{"name": "0123456789"}, KindOutOfRange, "$.name"},
		{"nested missing", map[string]any{"name": "a", "addr": map[string]any{}}, KindMissingRequired, "$.addr.city"},
		{"nested unknown", map[string]any{"name": "a", "addr": map[string]any{"city": "x", "no": 1}}, KindUnknownField, "$.addr.no"},
		{"nested type", map[string]any{"name": "a", "addr": map[string]any{"city": 1}}, KindTypeMismatch, "$.addr.city"},
		{"array item", map[string]any{"name": "a", "tags": []any{"ok", 3}}, KindTypeMismatch, "$.tags[1]"},
		{"array type", map[string]any{"name": "a", "tags": "x"}, KindTypeMismatch, "$.tags"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := Validate(testSchema(), c.args)
			if r == nil {
				t.Fatalf("expect rejection %s", c.kind)
			}
			if r.Kind != c.kind {
				t.Fatalf("kind: expect %s got %s (%v)", c.kind, r.Kind, r)
			}
			if r.Path != c.path {
				t.Fatalf("path: expect %s got %s", c.path, r.Path)
			}
		})
	}
}
