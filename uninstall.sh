#!/usr/bin/env bash
set -Eeuo pipefail

SERVICE_NAME="proxmox_exporter.service"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}"
APP_DIR="/opt/proxmox_exporter"

[[ "${EUID}" -eq 0 ]] || { echo "Run as root." >&2; exit 1; }

systemctl disable --now "${SERVICE_NAME}" 2>/dev/null || true
rm -f "${SERVICE_FILE}"
systemctl daemon-reload
systemctl reset-failed || true
rm -rf "${APP_DIR}"

echo "PROXMOX-EXPORTER has been removed."
