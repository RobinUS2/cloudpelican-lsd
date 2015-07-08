#!/bin/bash
curl -v -XPUT "http://cloud:pelican@localhost:1525/admin/config?key=$1&value=$2"
