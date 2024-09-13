#!/usr/bin/env bash
if [ -z "$1" ]; then
    echo "usage: $0 <username> <password>"
    exit 1
fi
_user="$1"
_pwd="$2"

_token=$(curl -X 'PUT' \
  "http://localhost:8080/login?username=$_user&password=$_pwd" \
  -H 'accept: application/json' | jq -r '.data.token')
if [ "$?" -ne "0" ]; then
    echo "Error logging in as $_user !"
    exit 1
fi
echo "$_token"
