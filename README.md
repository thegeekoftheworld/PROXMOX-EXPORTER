# PROXMOX-EXPORTER

A lightweight, native Prometheus metrics exporter written in C for Proxmox VE hosts. It exposes host, guest, storage, SMART, ZFS, Ceph, network, disk, memory, sensor, and RAID telemetry without requiring Python, containers, or a separate Proxmox API token.

The exporter runs directly on each Proxmox VE node, caches slower collectors in background threads, and serves Prometheus-compatible metrics over HTTP on port `9109` by default.

## Features

- Native C executable with a small runtime footprint
- Host CPU usage and I/O wait
- Memory, swap, load average, uptime, and process count
- Per-interface network counters and calculated throughput
- Per-device disk counters, throughput, and busy percentage
- Mounted filesystem capacity and usage
- Hardware temperature and fan sensors through Linux hwmon
- Linux MD RAID health and resync state
- SMART/NVMe health, temperature, wear, power-on hours, and media errors
- QEMU VM and LXC container status and resource metrics
- Guest CPU, memory, disk, network, I/O, and uptime metrics
- ZFS pool capacity and health metrics
- Ceph health and capacity metrics when Ceph is installed
- Background collector caches so slow checks do not run during every Prometheus scrape
- systemd service with automatic restart
- Installer that downloads and compiles the latest source directly from GitHub

## Requirements

- Proxmox VE or a compatible Debian-based Linux host
- Root access for installation and access to host-level telemetry
- Internet access to GitHub during installation or updates
- `systemd`

The installer adds these packages:

- `build-essential`
- `ca-certificates`
- `curl`
- `iproute2`
- `smartmontools`

ZFS, Ceph, QEMU, and LXC metrics are collected only when the corresponding commands and services are available on the host.

## Quick installation

Run as root:

```bash
curl -fsSL https://raw.githubusercontent.com/thegeekoftheworld/PROXMOX-EXPORTER/main/install.sh | bash
```

Or through `sudo`:

```bash
curl -fsSL https://raw.githubusercontent.com/thegeekoftheworld/PROXMOX-EXPORTER/main/install.sh | sudo bash
```

The installer will:

1. Install required packages.
2. Download the latest `proxmox_exporter.c` from the repository's `main` branch.
3. Compile it locally with GCC.
4. Back up an existing installation.
5. Install it under `/opt/proxmox_exporter`.
6. create and enable a systemd service.
7. Start the exporter and verify the `/metrics` endpoint.

Running the same installation command again updates the exporter to the latest source in GitHub.

## Custom port or branch

The default port is `9109`.

```bash
PORT=9200 curl -fsSL https://raw.githubusercontent.com/thegeekoftheworld/PROXMOX-EXPORTER/main/install.sh | bash
```

Because shell variables placed before `curl` do not automatically pass into the piped shell on every system, this form is more reliable:

```bash
curl -fsSL https://raw.githubusercontent.com/thegeekoftheworld/PROXMOX-EXPORTER/main/install.sh \
  | PORT=9200 bash
```

Install from another branch:

```bash
curl -fsSL https://raw.githubusercontent.com/thegeekoftheworld/PROXMOX-EXPORTER/main/install.sh \
  | BRANCH=development bash
```

## Updating

Use either command:

```bash
curl -fsSL https://raw.githubusercontent.com/thegeekoftheworld/PROXMOX-EXPORTER/main/install.sh | bash
```

```bash
/opt/proxmox_exporter/update.sh
```

To install the included update helper on an existing checkout:

```bash
sudo ./update.sh
```

## Endpoint

```text
http://PROXMOX-HOST-IP:9109/metrics
```

Test locally:

```bash
curl http://127.0.0.1:9109/metrics | head -50
```

## Prometheus configuration

Add each Proxmox node as a target:

```yaml
scrape_configs:
  - job_name: proxmox-native-exporter
    scrape_interval: 15s
    static_configs:
      - targets:
          - pve-node-1.example.net:9109
          - pve-node-2.example.net:9109
          - pve-node-3.example.net:9109
```

Reload or restart Prometheus after changing its configuration.

## Firewall

The exporter listens on all IPv4 interfaces. Restrict TCP port `9109` so it is reachable only from your Prometheus server or monitoring network.

Example using the Proxmox firewall or another firewall policy:

```text
Allow TCP 9109 from PROMETHEUS_SERVER_IP
Drop TCP 9109 from all other sources
```

The exporter does not include authentication or TLS. Use a firewall, VPN, private management network, or a secured reverse proxy when traffic crosses an untrusted network.

## Service management

```bash
systemctl status proxmox_exporter.service
systemctl restart proxmox_exporter.service
systemctl stop proxmox_exporter.service
journalctl -u proxmox_exporter.service -f
```

Installed files:

```text
/opt/proxmox_exporter/proxmox_exporter
/opt/proxmox_exporter/proxmox_exporter.c
/etc/systemd/system/proxmox_exporter.service
```

## Manual build

```bash
git clone https://github.com/thegeekoftheworld/PROXMOX-EXPORTER.git
cd PROXMOX-EXPORTER
make
./proxmox_exporter 9109
```

Equivalent GCC command:

```bash
gcc -O2 -Wall -Wextra -pthread \
  -o proxmox_exporter proxmox_exporter.c -lm
```

## Uninstall

From a cloned repository:

```bash
sudo ./uninstall.sh
```

Or manually:

```bash
systemctl disable --now proxmox_exporter.service
rm -f /etc/systemd/system/proxmox_exporter.service
systemctl daemon-reload
rm -rf /opt/proxmox_exporter
```

## Collector intervals

The current source uses separate background intervals for different classes of metrics:

| Collector | Default interval |
|---|---:|
| Host/fast metrics | 10 seconds |
| QEMU and LXC guests | 30 seconds |
| ZFS | 30 seconds |
| Ceph | 60 seconds |
| SMART/NVMe | 300 seconds |

Prometheus can scrape more frequently than the slower collectors; it receives the most recent cached result.

## Security notes

- The service runs as root because several collectors require privileged access.
- The HTTP endpoint has no authentication.
- Do not expose the metrics port directly to the public internet.
- Review source changes before running an installer directly from a branch you do not control.
- For reproducible production deployments, pin `BRANCH` to a tagged release or use a checksum-verified package.

## Troubleshooting

### Service does not start

```bash
systemctl status proxmox_exporter.service
journalctl -u proxmox_exporter.service --no-pager -n 100
```

### Port is already in use

```bash
ss -lntp | grep ':9109'
```

Install on another port:

```bash
curl -fsSL https://raw.githubusercontent.com/thegeekoftheworld/PROXMOX-EXPORTER/main/install.sh \
  | PORT=9200 bash
```

### SMART metrics are missing

Confirm that SMART data is available:

```bash
smartctl --scan
smartctl -a /dev/sda
```

Some RAID controllers require controller-specific `smartctl -d` arguments that the generic collector may not automatically detect.

### ZFS or Ceph metrics are missing

Verify that the host has the relevant tools and that they return data:

```bash
zpool list
zpool status
ceph status
```

### Guest metrics are missing

Verify the Proxmox command-line tools:

```bash
qm list
pct list
```

## Project layout

```text
PROXMOX-EXPORTER/
├── proxmox_exporter.c  Native exporter source
├── install.sh          Install or update from GitHub
├── update.sh           Convenience update command
├── uninstall.sh        Remove the exporter
├── Makefile            Manual build targets
├── README.md           Project documentation
├── LICENSE             MIT license
└── .gitignore
```

## License

MIT License. See [LICENSE](LICENSE).
