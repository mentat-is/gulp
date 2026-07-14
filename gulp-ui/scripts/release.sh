#!/usr/bin/env bash
set -euo pipefail

MAJOR_MINOR_FILE="${MAJOR_MINOR_FILE:-VERSION}"
if [[ -n "${1:-}" && "${1}" == "--help" ]]; then
  echo "Usage: scripts/release.sh [--major MAJOR] [--minor MINOR]"
  echo "Creates the next vMAJOR.MINOR.FIX tag. Defaults to MAJOR.MINOR from VERSION."
  exit 0
fi

MAJOR=""
MINOR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --major)
      MAJOR="${2:-}"
      shift 2
      ;;
    --minor)
      MINOR="${2:-}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

if [[ -z "${MAJOR}" || -z "${MINOR}" ]]; then
  if [[ ! -f "${MAJOR_MINOR_FILE}" ]]; then
    echo "1.0" > "${MAJOR_MINOR_FILE}"
  fi

  MM_RAW="$(cat "${MAJOR_MINOR_FILE}" | tr -d '[:space:]')"
  if [[ ! "${MM_RAW}" =~ ^[0-9]+\.[0-9]+$ ]]; then
    echo "VERSION must be MAJOR.MINOR"
    exit 1
  fi

  MAJOR="${MM_RAW%%.*}"
  MINOR="${MM_RAW##*.}"
fi

if [[ ! "${MAJOR}" =~ ^[0-9]+$ || ! "${MINOR}" =~ ^[0-9]+$ ]]; then
  echo "Major and minor must be numeric"
  exit 1
fi

git fetch --tags --force >/dev/null 2>&1 || true

PREFIX="v${MAJOR}.${MINOR}."
LATEST_FIX=$(
  git tag -l "${PREFIX}*" \
  | sed -nE "s/^v${MAJOR}\.${MINOR}\.([0-9]+)$/\1/p" \
  | sort -n | tail -n1
)

if [[ -z "${LATEST_FIX:-}" ]]; then
  NEXT_FIX=0
else
  NEXT_FIX=$((LATEST_FIX + 1))
fi

NEW_TAG="${PREFIX}${NEXT_FIX}"

git tag -a "${NEW_TAG}" -m "${NEW_TAG}"

echo "${NEW_TAG}"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  {
    echo "version=${NEW_TAG}"
    echo "major=${MAJOR}"
    echo "minor=${MINOR}"
    echo "fix=${NEXT_FIX}"
  } >> "$GITHUB_OUTPUT"
fi

if [[ "${PUSH_TAGS:-1}" == "1" ]]; then
  git push origin "${NEW_TAG}"
fi
