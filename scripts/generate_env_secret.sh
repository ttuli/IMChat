#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "文件不存在: $ENV_FILE"
    exit 1
fi

kubectl create secret generic im2-secret --from-env-file="$ENV_FILE" -n im2 --dry-run=client -o yaml > plain-im2-secret.yaml

kubeseal --controller-name=sealed-secrets --controller-namespace=kube-system --format=yaml < plain-im2-secret.yaml > im2-secret.yaml

rm plain-im2-secret.yaml

echo "✅ im2-secret.yaml 已更新"
