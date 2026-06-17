#!/usr/bin/env bash
set -Eeuo pipefail

# PROXMOX-EXPORTER installer/updater
#
# Install from GitHub:
#   curl -fsSL https://raw.githubusercontent.com/thegeekoftheworld/PROXMOX-EXPORTER/main/install.sh | bash
#
# Override settings when needed:
#   PORT=9200 BRANCH=main bash install.sh

REPO_OWNER="${REPO_OWNER:-thegeekoftheworld}"
REPO_NAME="${REPO_NAME:-PROXMOX-EXPORTER}"
BRANCH="${BRANCH:-main}"
PORT="${PORT:-9109}"

RAW_BASE="https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${BRANCH}"
SOURCE_URL="${SOURCE_URL:-${RAW_BASE}/proxmox_exporter.c}"

APP_DIR="${APP_DIR:-/opt/proxmox_exporter}"
SRC_FILE="${APP_DIR}/proxmox_exporter.c"
BIN_FILE="${APP_DIR}/proxmox_exporter"
SERVICE_NAME="proxmox_exporter.service"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}"

TEMP_DIR=""
BACKUP_DIR=""

log() { printf '==> %s\n' "$*"; }
warn() { printf 'WARNING: %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

cleanup() {
    [[ -z "${TEMP_DIR}" || ! -d "${TEMP_DIR}" ]] || rm -rf "${TEMP_DIR}"
}
trap cleanup EXIT

[[ "${EUID}" -eq 0 ]] || die "Run this installer as root. Example: curl -fsSL ${RAW_BASE}/install.sh | sudo bash"
[[ "${PORT}" =~ ^[0-9]+$ ]] || die "PORT must be numeric."
(( PORT >= 1 && PORT <= 65535 )) || die "PORT must be between 1 and 65535."

export DEBIAN_FRONTEND=noninteractive

log "Installing required packages"
apt-get update
apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    iproute2 \
    smartmontools

TEMP_DIR="$(mktemp -d /tmp/proxmox-exporter.XXXXXX)"
TEMP_SOURCE="${TEMP_DIR}/proxmox_exporter.c"
TEMP_BINARY="${TEMP_DIR}/proxmox_exporter"

log "Downloading latest exporter source from ${SOURCE_URL}"
curl --fail --silent --show-error --location \
    --retry 3 --retry-delay 2 --connect-timeout 15 --max-time 180 \
    "${SOURCE_URL}" -o "${TEMP_SOURCE}"

[[ -s "${TEMP_SOURCE}" ]] || die "Downloaded source is empty."
grep -qE '^[[:space:]]*#include[[:space:]]*[<"]' "${TEMP_SOURCE}" || \
    die "Downloaded file does not appear to be C source."
grep -q 'int main' "${TEMP_SOURCE}" || die "Downloaded source does not contain main()."

log "Compiling exporter"
gcc -O2 -Wall -Wextra -pthread \
    -o "${TEMP_BINARY}" "${TEMP_SOURCE}" -lm
[[ -x "${TEMP_BINARY}" ]] || die "Compilation did not produce an executable binary."

install -d -m 0755 "${APP_DIR}"
BACKUP_DIR="${APP_DIR}/backup-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${BACKUP_DIR}"

if [[ -f "${BIN_FILE}" ]]; then
    log "Backing up current installation to ${BACKUP_DIR}"
    cp -a "${BIN_FILE}" "${BACKUP_DIR}/"
    [[ ! -f "${SRC_FILE}" ]] || cp -a "${SRC_FILE}" "${BACKUP_DIR}/"
    [[ ! -f "${SERVICE_FILE}" ]] || cp -a "${SERVICE_FILE}" "${BACKUP_DIR}/"
fi

if systemctl cat "${SERVICE_NAME}" >/dev/null 2>&1; then
    log "Stopping existing service"
    systemctl stop "${SERVICE_NAME}" || true
    systemctl reset-failed "${SERVICE_NAME}" || true
fi

log "Installing source and binary"
install -m 0644 "${TEMP_SOURCE}" "${SRC_FILE}"
install -m 0755 "${TEMP_BINARY}" "${BIN_FILE}"

log "Writing systemd service"
cat > "${SERVICE_FILE}" <<SERVICE_EOF
[Unit]
Description=Native Proxmox Prometheus Exporter
Documentation=https://github.com/${REPO_OWNER}/${REPO_NAME}
Wants=network-online.target
After=network-online.target pve-cluster.service
ConditionPathIsExecutable=${BIN_FILE}

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=${APP_DIR}
ExecStart=${BIN_FILE} ${PORT}
Restart=on-failure
RestartSec=5
TimeoutStartSec=20
TimeoutStopSec=20
KillSignal=SIGTERM
KillMode=control-group
SendSIGKILL=yes
NoNewPrivileges=yes
PrivateTmp=yes
ProtectHome=yes
LockPersonality=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
SystemCallArchitectures=native
StandardOutput=journal
StandardError=journal
SyslogIdentifier=proxmox_exporter

[Install]
WantedBy=multi-user.target
SERVICE_EOF

log "Reloading systemd and starting exporter"
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
systemctl restart "${SERVICE_NAME}"
sleep 2

if ! systemctl is-active --quiet "${SERVICE_NAME}"; then
    journalctl -u "${SERVICE_NAME}" --no-pager -n 100 || true
    die "The exporter service failed to start. A backup is available in ${BACKUP_DIR}."
fi

log "Testing metrics endpoint"
if ! curl --fail --silent --show-error --connect-timeout 5 --max-time 30 \
    "http://127.0.0.1:${PORT}/metrics" >/dev/null; then
    journalctl -u "${SERVICE_NAME}" --no-pager -n 100 || true
    die "The service is running, but the metrics endpoint did not respond."
fi

PRIMARY_NIC="$(ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev") {print $(i+1); exit}}' || true)"
PRIMARY_IP=""
if [[ -n "${PRIMARY_NIC}" ]]; then
    PRIMARY_IP="$(ip -4 -o addr show dev "${PRIMARY_NIC}" scope global 2>/dev/null | awk '{split($4,a,"/"); print a[1]; exit}' || true)"
fi

cat <<SUMMARY

PROXMOX-EXPORTER installation completed successfully.

Binary:   ${BIN_FILE}
Source:   ${SRC_FILE}
Service:  ${SERVICE_FILE}
Port:     ${PORT}
Interface:${PRIMARY_NIC:+ ${PRIMARY_NIC}}
IP:       ${PRIMARY_IP:+ ${PRIMARY_IP}}

Local metrics endpoint:
  http://127.0.0.1:${PORT}/metrics
SUMMARY

if [[ -n "${PRIMARY_IP}" ]]; then
    printf '\nNetwork metrics endpoint:\n  http://%s:%s/metrics\n' "${PRIMARY_IP}" "${PORT}"
fi

cat <<COMMANDS

Useful commands:
  systemctl status ${SERVICE_NAME}
  journalctl -u ${SERVICE_NAME} -f
  curl http://127.0.0.1:${PORT}/metrics | head -50

Run the installer again at any time to download, compile, and install the latest source from GitHub.
COMMANDS
