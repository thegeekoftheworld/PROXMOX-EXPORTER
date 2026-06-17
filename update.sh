#!/usr/bin/env bash
set -Eeuo pipefail

REPO_OWNER="${REPO_OWNER:-thegeekoftheworld}"
REPO_NAME="${REPO_NAME:-PROXMOX-EXPORTER}"
BRANCH="${BRANCH:-main}"
INSTALLER_URL="https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${BRANCH}/install.sh"

[[ "${EUID}" -eq 0 ]] || { echo "Run as root." >&2; exit 1; }
exec bash <(curl -fsSL --retry 3 "${INSTALLER_URL}")
