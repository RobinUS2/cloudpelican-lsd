// Statistics tool
// @author Robin Verlangen

package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	"github.com/mgutz/ansi"
)

type Statistics struct {
	verticalSep    string
	horizontalSep  string
	colPad         int
	terminalWidth  int
	terminalHeight int
}

func (s *Statistics) loadTerminalDimensions() {
	cmd := exec.Command("stty", "size")
	cmd.Stdin = os.Stdin
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	str := strings.TrimSpace(string(out))
	split := strings.Split(str, " ")
	height, _ := strconv.ParseInt(split[0], 10, 0)
	width, _ := strconv.ParseInt(split[1], 10, 0)
	s.terminalHeight = int(height)
	s.terminalWidth = int(width)
	if verbose {
		log.Println("Terminal dimension %dx%d (WxH)", s.terminalWidth, s.terminalHeight)
	}
}

func (s *Statistics) RenderChart(filter *Filter, inputData map[int]map[int64]int64) (string, error) {
	// Random data (primary is top, secondary is filled, e.g. errors)
	data := make([]int64, 0)
	dataSecondary := make([]int64, 0)
	metricId := 1
	if inputData[metricId] == nil || len(inputData[metricId]) < 1 {
		return "", errors.New("Metrics not available for this filter")
	}
	// @todo Sort by TS
	// To store the keys in slice in sorted order
	var keys []int
	for ts, _ := range inputData[metricId] {
		keys = append(keys, int(ts))
	}
	sort.Ints(keys)
	for _, k := range keys {
		val := inputData[metricId][int64(k)]
		data = append(data, val)
		dataSecondary = append(dataSecondary, 0) // No error data yet
	}

	// Width and height for chart
	dataWidth := len(data)
	if dataWidth > s.terminalWidth {
		log.Println("Warning, truncating data to match terminal width")
		data = data[:s.terminalWidth-1]
		dataWidth = len(data)
		// @todo Compress data (merge data points and get sums in order to fit in screen)
	}
	maxHeight := s.terminalHeight - 4 // remove some for padding
	maxWidth := int(math.Max(float64(dataWidth), float64(s.terminalWidth)))

	// Scan for min and max
	minVal := int64(math.MaxInt64)
	maxVal := int64(math.MinInt64)
	for _, val := range data {
		if val < minVal {
			minVal = val
		}
		if val > maxVal {
			maxVal = val
		}
	}

	// Dynamic column padding
	s.colPad = int((maxWidth - len(data)) / len(data))

	// Color codes
	colorRed := ansi.ColorCode("red")
	colorGreen := ansi.ColorCode("green")
	resetColor := ansi.ColorCode("reset")

	// Start to build chart (top to bottom)
	var buf bytes.Buffer
	for line := maxHeight; line >= 0; line-- {
		// Min line val (10/30)=0.3*10
		minLineVal := int64(float64(line) / (float64(maxHeight) / float64(maxVal)))

		// Iterate columns
		for col := 0; col < len(data); col++ {
			// Determine what to write
			if col == 0 && line != 0 {
				// Left axis
				buf.WriteString(s.verticalSep)
			} else if line == 0 {
				// Bottom axis
				buf.WriteString(s.horizontalSep)
			} else {
				// Data point
				colVal := data[col]
				secondaryColVal := dataSecondary[col]

				// Print?
				if colVal >= minLineVal {
					if secondaryColVal >= minLineVal {
						buf.WriteString(fmt.Sprintf("%s%s%s", colorRed, "*", resetColor))
					} else {
						buf.WriteString(fmt.Sprintf("%s%s%s", colorGreen, "o", resetColor))
					}
				} else {
					buf.WriteString(" ")
				}
			}

			// Padding
			if line != 0 {
				buf.WriteString(strings.Repeat(" ", s.colPad)) // Column padding
			} else {
				buf.WriteString(strings.Repeat(s.horizontalSep, s.colPad)) // Horizontal axis padding
			}
		}
		buf.WriteString("\n") // Close previous line
	}
	buf.WriteString("\n") // Final whiteline

	return buf.String(), nil
}

func newStatistics() *Statistics {
	s := &Statistics{
		verticalSep:   "|",
		horizontalSep: "_",
		colPad:        3,
	}
	s.loadTerminalDimensions()
	return s
}
