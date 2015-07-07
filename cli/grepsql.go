// Convert grep-like commands to SQL for ClouePelican
// Example: cat errors | grep -v 404 | grep -i checkout | grep -E "(100|200)"
// SELECT _raw FROM errors WHERE _raw NOT LIKE '%404%' AND LOWER(_raw) LIKE LOWER('checkout') AND REGEXP_MATCH(_raw, '(100|200)') ORDER BY ts ASC LIMIT 1000
// @author Robin Verlangen

package main

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"text/scanner"
)

type GrepSQL struct {
	input string
}

func (g *GrepSQL) Parse() (string, error) {
	// Tokenize
	var s scanner.Scanner
	s.Init(strings.NewReader(g.input))
	tok := s.Scan()
	var previousTokens []string = make([]string, 0)
	var i int = 0
	for tok != scanner.EOF {
		// do something with tok
		tok = s.Scan()
		token := s.TokenText()
		log.Println(token)

		// Keep track of the previous tokens
		previousTokens = append(previousTokens, token)

		// Counter
		i++
	}

	// Tokens
	if len(previousTokens) < 1 {
		return "", errors.New("Invalid input")
	}

	// Validate & fetch filter
	filter, filterE := supervisorCon.FilterByName(previousTokens[0])
	if filterE != nil {
		return "", filterE
	}

	// Begin builder query
	var qBuf bytes.Buffer
	qBuf.WriteString("SELECT")
	qBuf.WriteString(" ")
	qBuf.WriteString("_raw")
	qBuf.WriteString(" ")
	qBuf.WriteString("FROM")
	qBuf.WriteString(" ")
	qBuf.WriteString(filter.GetSearchTableName())

	// Done
	return qBuf.String(), nil
}

func newGrepSQL(input string) *GrepSQL {
	return &GrepSQL{
		input: input,
	}
}
