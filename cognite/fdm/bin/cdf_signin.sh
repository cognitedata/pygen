#!/bin/bash
set -e

. "${0%/*}/_functions.sh" "$1"


CDF_CLUSTER=$(cat "$CONFIG" | extract 'cdf_cluster')
PROJECT=$(cat "$CONFIG" | extract 'project')
TENANT_ID=$(cat "$CONFIG" | extract 'tenant_id')
CLIENT_ID=$(cat "$CONFIG" | extract 'client_id')
CLIENT_SECRET=$(cat "$CONFIG" | extract 'client_secret')

if [ -z "$CLIENT_SECRET" ]; then
  SECRET_OR_DEVICE_CODE="--device-code"
else
  SECRET_OR_DEVICE_CODE="--client-secret='$CLIENT_SECRET'"
fi

exec cdf signin "$PROJECT" --tenant="$TENANT_ID" --cluster="$CDF_CLUSTER" --client-id="$CLIENT_ID" "$SECRET_OR_DEVICE_CODE"
