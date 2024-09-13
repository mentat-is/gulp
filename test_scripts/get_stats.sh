#!/usr/bin/env bash
# login as admin
_d=$(dirname $0)
_token=$($_d/login.sh admin admin)
if [ "$?" -ne "0" ]; then
    echo $_token
    exit 1
fi

curl -X 'POST' \
    "http://127.0.0.1:8080/stats_list" \
    -H 'token: $_token' \
    -H 'accept: application/json'
