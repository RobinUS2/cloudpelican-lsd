#!/bin/bash
export GOPATH=`pwd`
go get -u "github.com/carmark/pseudo-terminal-go/terminal"
go build .
