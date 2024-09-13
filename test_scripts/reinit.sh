#!/usr/bin/env bash
# drops and resets both elasticsearch and collaboration instances

function usage {
  echo 'reinitializes (a running instance of) gulp.'
  echo
  echo 'usage:' "$0" '-i <elastic index> [-c to clear_elastic_only] [-h proto://host:port, default is http://localhost:8080]'
  echo
  echo 'ADMIN_USERNAME and ADMIN_PASSWORD may be set in the environment to override admin credentials (admin/admin).'
}

_INDEX=""
_CLEAR_ELASTIC_ONLY=0
_HOST="http://localhost:8080"
_ADMIN_USERNAME=${ADMIN_USERNAME:-admin}
_ADMIN_PASSWORD=${ADMIN_PASSWORD:-admin}

while getopts "i:c:h:" arg; do
  case $arg in
  h)
    _HOST="${OPTARG}"
    ;;
  c)
    _CLEAR_ELASTIC_ONLY=1
    ;;
  i)
    _INDEX="${OPTARG}"
    ;;
  *)
    usage "$0"
    exit 1
    ;;
  esac
done

# print parameters
echo "[.] index: $_INDEX"
echo "[.] clear elastic only: $_CLEAR_ELASTIC_ONLY"
echo "[.] host: $_HOST"

if [ -z "$_INDEX" ]; then
  usage "$0"
  exit 1
fi

# login as admin
_d=$(dirname $0)
_TOKEN=$($_d/login.sh $_ADMIN_USERNAME $_ADMIN_PASSWORD)
if [ "$?" -ne "0" ]; then
    echo $_TOKEN
    exit 1
fi


curl -X 'DELETE' \
    "$_HOST/elastic_init?index=$_INDEX" \
    -H "token: $_TOKEN" \
    -H 'accept: application/json'
if [ "$?" -ne "0" ]; then
    exit 1
fi

if [ "$_CLEAR_ELASTIC_ONLY" -eq 1 ]; then
    exit 0
fi

curl -X 'DELETE' \
    "$_HOST/collab_init" \
    -H "token: $_TOKEN" \
    -H 'accept: application/json'
