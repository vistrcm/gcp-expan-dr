# gcp-expan-dr

Automatic disk expansion tool for GCP instances that monitors root filesystem usage and expands disks when they exceed a specified threshold.

## How It Works

1. Runs on a GCP VM and identifies disks with the `gcp-expan-dr-trigger-usage` label
2. For labeled disks mounted as root (/), checks if usage exceeds the threshold value in the label
3. When threshold is exceeded, expands the disk by 10GB (or specified amount)
4. After disk expansion, extends the partition and filesystem automatically

## Prerequisites

- Running on a GCP VM with ext4 root filesystem
- VM service account permissions:
  - Compute Viewer (`roles/compute.viewer`)
  - Compute Storage Admin (`roles/compute.storageAdmin`)
- Required tools: `growpart`, `resize2fs`

## Installation

```bash
# Build locally
go build -o gcp-expan-dr

# Build for GCP Linux VMs
GOOS=linux GOARCH=amd64 go build -o gcp-expan-dr
```

## Usage

```bash
# Run with default settings (10GB expansion)
./gcp-expan-dr

# Run with custom expansion size
GCP_EXPAN_DR_SIZE_GB=20 ./gcp-expan-dr
```

## Configuration

- **Disk Labels**: Add `gcp-expan-dr-trigger-usage=80` label to disks (threshold percentage)
- **Environment Variables**:
  - `GCP_EXPAN_DR_SIZE_GB`: Expansion size in GB (default: 10)