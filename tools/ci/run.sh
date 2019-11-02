#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

set -x

sleep 10

./manage.sh "$@"
