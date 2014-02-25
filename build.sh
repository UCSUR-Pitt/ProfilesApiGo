#!/bin/bash
rm bin/profiles_api
go install github.com/ProvidencePlan/profiles_api
./bin/profiles_api cp_dev.json :8080
