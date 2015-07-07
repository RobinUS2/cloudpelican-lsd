// Convert grep-like commands to SQL for ClouePelican
// Example: cat errors | grep -v 404 | grep -i checkout | grep -E "(100|200)"
// SELECT _raw FROM errors WHERE _raw NOT LIKE '%404%' AND LOWER(_raw) LIKE LOWER('checkout') AND REGEXP_MATCH(_raw, '(100|200)') ORDER BY ts ASC LIMIT 1000
// @author Robin Verlangen

package main

type GrepSQL struct {
	input string
}

func (g *GrepSQL) Parse() (string, error) {
	return "test", nil
}

func newGrepSQL(input string) *GrepSQL {
	return &GrepSQL{
		input: input,
	}
}
