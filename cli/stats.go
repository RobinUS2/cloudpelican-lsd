// Statistics tool
// @author Robin Verlangen

package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

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

func (s *Statistics) GetChart(filter *Filter) string {
	maxHeight := s.terminalHeight - 4 // remove some for padding
	maxWidth := int(math.Max(24, float64(s.terminalWidth)))

	// Random data (primary is top, secondary is filled, e.g. errors)
	data := make([]int32, 0)
	dataSecondary := make([]int32, 0)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 24; i++ {
		v := rand.Int31n(100)
		data = append(data, v)
		v2 := int32(math.Min(float64(v), float64(rand.Int31n(50))))
		dataSecondary = append(dataSecondary, v2)
	}

	// Scan for min and max
	minVal := int32(math.MaxInt32)
	maxVal := int32(math.MinInt32)
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
		minLineVal := int32(float32(line) / (float32(maxHeight) / float32(maxVal)))

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

				// Normalize value (e.g. height max is 10, max value is 50, this value is 25, needs to be 5)
				//normalizedColVal := int((float32(maxHeight) / float32(maxVal)) * float32(colVal))

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

	return buf.String()
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
