# GCP Disk Auto-Expansion

A CLI application for GCP instances that monitors root filesystem usage and automatically expands disks when they exceed a specified threshold.

## Features

- Verifies if the application is running on a Google Cloud Platform VM
- Displays basic instance information:
  - Instance ID
  - Instance Name
  - Project ID
  - Zone
- Lists all attached disks with details:
  - Device name
  - Disk source
  - Disk type (Persistent Disk or Local SSD)
  - Boot disk status
- Monitors disks with the `gcp-expan-dr-trigger-usage` label:
  - Checks if the disk is mounted as the root filesystem
  - Compares current usage percentage with the threshold in the label
  - Automatically expands disks by 10GB when they exceed the threshold
  - Monitors resize operations and reports success or failure 
  - Provides real-time progress updates during disk resizing
  - Automatically extends partitions using growpart
  - Automatically extends ext4 filesystems using resize2fs

## Prerequisites

The application requires:

1. The VM's service account to have the following IAM permissions:
   - **Compute Viewer** (`roles/compute.viewer`) - For viewing instance and disk information
   - **Compute Storage Admin** (`roles/compute.storageAdmin`) or permission to update disks (`compute.disks.update`) - For resizing disks

2. For filesystem extension:
   - `growpart`
   - `resize2fs`
   - `fdisk`


## Building

Build for your current platform:
```
go build -o gcp-expan-dr
```

Build for Linux (for GCP VMs):
```
GOOS=linux GOARCH=amd64 go build -o gcp-expan-dr-linux-amd64
```

## Usage

Simply run the binary on a GCP VM:
```
./gcp-expan-dr
```

If the application is not running on a GCP VM, it will exit with an error message.

### Configuration Options

The application can be configured using environment variables:

- `GCP_EXPAN_DR_SIZE_GB`: Sets the amount to expand disks by when they exceed the threshold (default: 10GB)

Example:
```
GCP_EXPAN_DR_SIZE_GB=20 ./gcp-expan-dr
```