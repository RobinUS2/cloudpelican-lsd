// Convert grep-like commands to SQL for ClouePelican
// Example: cat errors | grep -v 404 | grep -i checkout | grep -e "(100|200)"
// SELECT _raw FROM errors WHERE _raw NOT LIKE '%404%' AND LOWER(_raw) LIKE LOWER('checkout') AND REGEXP_MATCH(_raw, '(100|200)') ORDER BY ts ASC LIMIT 1000
// @author Robin Verlangen

// @todo Support color

package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
	"text/scanner"
)

type GrepSQL struct {
	input string
	cmds  []GrepCmd
}

type GrepCmd struct {
	flags   map[string]bool // v=exclude, i=case-insensitive, e=regex
	pattern string
}

func (w *GrepCmd) Where() string {
	if verbose {
		log.Printf("%v", w)
	}

	// Column
	var column string = "_raw"

	// Exclude?
	var notSuffix string = ""
	if w.flags["v"] {
		notSuffix = "NOT "
	}

	// Regex?
	if w.flags["e"] {
		if w.flags["i"] && !strings.Contains(w.pattern, "(?i)") {
			// Inject case insensitive into the pattern
			w.pattern = fmt.Sprintf("(?i)%s", w.pattern)
		}
		return fmt.Sprintf("%sREGEXP_MATCH(%s, '%s')", notSuffix, column, w.pattern)
	}

	// Case sensitive?
	if w.flags["i"] {
		return fmt.Sprintf("LOWER(%s) %sLIKE LOWER('%%%s%%')", column, notSuffix, w.pattern)
	}

	// Regular matching
	return fmt.Sprintf("%s %sLIKE '%%%s%%'", column, notSuffix, w.pattern)
}

func (g *GrepSQL) Parse() (string, error) {
	// Tokenize
	var s scanner.Scanner
	s.Init(strings.NewReader(g.input))
	tok := s.Scan()
	var previousTokens []string = make([]string, 0)
	var i int = 0
	var currentCmd *GrepCmd = nil
	for tok != scanner.EOF {
		// Get token
		tok = s.Scan()
		token := s.TokenText()
		if verbose {
			log.Println(fmt.Sprintf("\n\nToken: %s", token))
		}

		// Create list of grep commands
		if i > 0 {
			if verbose {
				log.Printf("prev %s", previousTokens[i-1])
				log.Printf("currentCmd %v", currentCmd)
			}

			// Begin of command
			if previousTokens[i-1] == "|" {
				// Previous token is pipe, this must be grep
				if token != "grep" {
					return "", errors.New(fmt.Sprintf("Invalid token %s", token))
				}

				// Init
				if verbose {
					log.Printf("New token command")
				}
				currentCmd = newGrepCmd()
			} else if previousTokens[i-1] == "-" && currentCmd != nil {
				// Flag
				if token != "v" && token != "e" && token != "i" {
					return "", errors.New(fmt.Sprintf("Invalid flag %s", token))
				}
				// Set flag
				if verbose {
					log.Printf("Flag %s", token)
				}
				currentCmd.flags[token] = true
			} else if token != "-" && currentCmd != nil {
				if verbose {
					log.Printf("pattern %s", token)
				}
				// Pattern
				currentCmd.pattern = strings.Trim(token, "\"'")

				// Add to list
				g.cmds = append(g.cmds, *currentCmd)

				// Reset
				currentCmd = nil
			}
		}

		// Keep track of the previous tokens
		previousTokens = append(previousTokens, token)

		// Counter
		i++
	}

	// Tokens
	if len(previousTokens) < 1 {
		return "", errors.New("Invalid input")
	}

	// Print structure
	if verbose {
		log.Printf("%v", g)
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

	// Where part?
	if len(g.cmds) > 0 {
		qBuf.WriteString(" ")
		qBuf.WriteString("WHERE")
		qBuf.WriteString(" ")

		// Build filter
		var wheres []string = make([]string, 0)
		for _, cmd := range g.cmds {
			wheres = append(wheres, cmd.Where())
		}
		qBuf.WriteString(strings.Join(wheres, " AND "))
	}

	// Done
	return qBuf.String(), nil
}

func newGrepCmd() *GrepCmd {
	return &GrepCmd{
		flags: make(map[string]bool),
	}
}

func newGrepSQL(input string) *GrepSQL {
	return &GrepSQL{
		input: input,
		cmds:  make([]GrepCmd, 0),
	}
}
