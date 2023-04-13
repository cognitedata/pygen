#!/bin/bash
set -e

extract () {
  ATTR="$1"
  cat | awk "\$1 == \"${ATTR}\" {print \$3}" | trim_quotes
}

trim_quotes () {
  cat | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//"
}

CONFIG="${DM_CLIENTS_CONFIG:-${1:-settings.toml}}"
