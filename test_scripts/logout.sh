#!/usr/bin/env bash
if [ -z "$1" ]; then
    echo "usage: $0 <token>"
    exit 1
fi
_token="$1"

curl -X 'PUT' \
  "http://localhost:8080/logout" \
  -H 'token: $_token' \
  -H 'accept: application/json'
if [ "$?" -ne "0" ]; then
    echo "Error logging out token $_token !"
    exit 1
fi
echo "session $_token is no more valid!"
