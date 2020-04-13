#!/bin/bash
go test -v -p 1 -timeout 120s github.com/uol/timeline/tests/http
go test -v -p 1 -timeout 120s github.com/uol/timeline/tests/opentsdb
