#!/bin/bash
export GOPATH=`pwd`
go get ./...
go build .
