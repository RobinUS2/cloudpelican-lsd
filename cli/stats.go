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
	colorRed       string
	colorGreen     string
	colorReset     string
	colorEnabled   bool
}

func (s *Statistics) loadTerminalDimensions() {
	cmd := exec.Command("stty", "size")
	cmd.Stdin = os.Stdin
	out, err := cmd.Output()
	if err != nil {
		log.Printf("Failed to determine terminal dimensions: %s", err)
		log.Printf("Disabling color")
		s.colorEnabled = false
		s.terminalWidth = 100
		s.terminalHeight = 50
		if verbose {
			log.Printf("Terminal dimension %dx%d (WxH)", s.terminalWidth, s.terminalHeight)
		}
		return
	}
	str := strings.TrimSpace(string(out))
	split := strings.Split(str, " ")
	height, _ := strconv.ParseInt(split[0], 10, 0)
	width, _ := strconv.ParseInt(split[1], 10, 0)
	s.terminalHeight = int(height)
	s.terminalWidth = int(width)
	if verbose {
		log.Printf("Terminal dimension %dx%d (WxH)", s.terminalWidth, s.terminalHeight)
	}
}

func (s *Statistics) RenderChart(filter *Filter, inputData map[int]map[int64]int64, flags map[string]bool) (string, error) {
	// Random data (primary is top, secondary is filled, e.g. errors)
	data := make([]int64, 0)
	dataSecondary := make([]int64, 0)

	// Metric source IDs
	metricId := 1
	secondaryMetricId := 2

	// Colors
	primaryColor := "green"
	secondaryColor := "red"
	if s.colorEnabled {
		primaryColor = "reset"
		secondaryColor = "reset"
	}

	// Primary sign
	primarySign := "o"
	secondarySign := "*"

	// Flags
	if flags["hide_error"] {
		secondaryMetricId = -1 // Disable errors
	}
	if flags["hide_regular"] {
		// Swap error metric to primary
		metricId = secondaryMetricId
		primaryColor = secondaryColor
		primarySign = secondarySign
		secondaryMetricId = -1 // Disable secondary
	}

	// Validate
	if inputData[metricId] == nil || len(inputData[metricId]) < 1 {
		return "", errors.New("Metrics not available for this filter")
	}
	// To store the keys in slice in sorted order
	var keys []int
	for ts, _ := range inputData[metricId] {
		keys = append(keys, int(ts))
	}
	sort.Ints(keys)
	for _, k := range keys {
		val := inputData[metricId][int64(k)]
		data = append(data, val)

		// Errors
		var secVal int64 = 0
		if secondaryMetricId > 0 && inputData[secondaryMetricId] != nil {
			secVal = inputData[secondaryMetricId][int64(k)]
		}
		dataSecondary = append(dataSecondary, secVal)
	}

	// Width and height for chart
	dataWidth := len(data)
	maxDataLen := s.terminalWidth - 1
	if dataWidth > maxDataLen {
		log.Println("Warning, truncating data to match terminal width")
		data = data[len(data)-maxDataLen:]
		dataSecondary = dataSecondary[len(dataSecondary)-maxDataLen:]
		dataWidth = len(data)
		// @todo Compress data (merge data points and get sums in order to fit in screen)
	}
	maxHeight := int(math.Min(float64(20), float64(s.terminalHeight-4))) // remove some for padding
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
	var currentColor string = "reset"
	var colorStr string = ""

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
				currentColor, colorStr = s.colorStr(currentColor, "reset", s.verticalSep)
				buf.WriteString(colorStr)
			} else if line == 0 {
				// Bottom axis
				currentColor, colorStr = s.colorStr(currentColor, "reset", s.horizontalSep)
				buf.WriteString(colorStr)
			} else {
				// Data point
				colVal := data[col]
				secondaryColVal := dataSecondary[col]

				// Print?
				if colVal >= minLineVal {
					if secondaryColVal >= minLineVal {
						currentColor, colorStr = s.colorStr(currentColor, secondaryColor, secondarySign)
						buf.WriteString(colorStr)
					} else {
						currentColor, colorStr = s.colorStr(currentColor, primaryColor, primarySign)
						buf.WriteString(colorStr)
					}
				} else {
					buf.WriteString(" ")
				}
			}

			// Padding
			if line != 0 {
				buf.WriteString(strings.Repeat(" ", s.colPad)) // Column padding
			} else {
				// Horizontal axis padding
				currentColor, colorStr = s.colorStr(currentColor, "reset", strings.Repeat(s.horizontalSep, s.colPad))
				buf.WriteString(colorStr)
			}
		}
		buf.WriteString("\n") // Close previous line
	}
	buf.WriteString("\n")         // Final whiteline
	buf.WriteString(s.colorReset) // Reset color

	return buf.String(), nil
}

func (s *Statistics) colorStr(currentColor string, desiredColorName string, str string) (string, string) {
	if currentColor == desiredColorName {
		return currentColor, str
	}
	colorStr := ""
	if desiredColorName == "green" {
		colorStr = s.colorGreen
	} else if desiredColorName == "red" {
		colorStr = s.colorRed
	} else if desiredColorName == "reset" {
		colorStr = s.colorReset
	}
	return desiredColorName, fmt.Sprintf("%s%s", colorStr, str)
}

func newStatistics() *Statistics {
	s := &Statistics{
		verticalSep:   "|",
		horizontalSep: "_",
		colPad:        3,
		colorEnabled:  true,
		colorRed:      ansi.ColorCode("red"),
		colorGreen:    ansi.ColorCode("green"),
		colorReset:    ansi.ColorCode("reset"),
	}
	s.loadTerminalDimensions()
	return s
}
