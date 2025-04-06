package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/compute/metadata"
	"google.golang.org/api/compute/v1"
)

const (
	thresholdLabelName   = "gcp-expan-dr-trigger-usage"
	defaultExpandSizeGB  = 10
	diskOperationTimeout = 5 * time.Minute // Maximum wait time for operation
	reCheckInterval      = 3 * time.Second
	commandTimeout       = 60 * time.Second
)

type instanceMeta struct {
	instanceID   string
	instanceName string
	projectID    string
	zoneName     string
}

func main() {
	// Check if running on GCP
	if !metadata.OnGCE() {
		log.Fatal("Error: This application must be run on Google Cloud Platform")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	instMeta, err := getMetadata(ctx)
	if err != nil {
		log.Fatalf("Error retrieving metadata: %v", err)
	}

	logMeta(instMeta)

	// Create compute service for GCP operations
	computeService, err := compute.NewService(ctx)
	if err != nil {
		log.Fatalf("can't create compute service: %v", err)
	}

	// Get the disks attached to this instance
	disks, err := getAttachedDisks(computeService, instMeta.projectID, instMeta.zoneName, instMeta.instanceName)
	if err != nil {
		log.Fatalf("Error retrieving attached disks: %v", err)
	}

	logDiskInfo(disks)

	disk2device, err := mapGoogleDisk2Devices(disks)
	if err != nil {
		log.Fatalf("Error retrieving disk to device mapping: %v", err)
	}

	// Get detailed disk information for those with the trigger label
	labeledDisks, err := getDisksWithTriggerLabel(computeService, instMeta.projectID, instMeta.zoneName, disks)
	if err != nil {
		log.Fatalf("error retrieving labeled disk details: %v", err)
	}

	if len(labeledDisks) == 0 {
		log.Println("No disks with the gcp-expan-dr-trigger-usage label found")
		os.Exit(0)
	}

	// Get mounted filesystems info once
	mntInfo, err := getMountInfo()
	if err != nil {
		log.Printf("Warning: Error getting mount information: %v", err)
	}

	disksNeedingExpansion, err := filterNeedExpansion(labeledDisks, disks, disk2device, mntInfo)
	if err != nil {
		log.Fatalf("can't filter disks: %v", err)
	}

	if len(disksNeedingExpansion) <= 0 {
		log.Printf("No disks need expansion\n")
		os.Exit(0)
	}

	err = resizeGCPDisk(computeService, disksNeedingExpansion, instMeta)
	if err != nil {
		log.Fatalf("can't resize GCP disk: %v\n", err)
	}

	err = growPartition(mntInfo)
	if err != nil {
		log.Fatalf("can't grow partition: %v\n", err)
	}

	// Get updated filesystem size for reporting
	fsUsage, err := getFilesystemUsage("/")
	if err != nil {
		log.Fatalf("failed go get failsystem usage: %v\n", err)
	}
	newSizeGB := float64(fsUsage.Total) / 1024 / 1024 / 1024
	log.Printf("partition and filesystem extended successfully. New size: %.1f GB\n", newSizeGB)
}

func growPartition(mntInfo mountInfo) error {
	// Check filesystem type
	fsType, err := getFilesystemType("/", mntInfo)
	if err != nil {
		return fmt.Errorf("could not determine root filesystem type: %w", err)
	}
	if fsType != "ext4" {
		return fmt.Errorf("root filesystem %q is unsupported. Supported fs: ext4", fsType)
	}

	disk, partition, err := extractDiskAndPartition(mntInfo.rootDevice)
	if err != nil {
		return fmt.Errorf("failed to parse disk and partition: %w", err)
	}

	log.Printf("  Running: growpart %s %s\n", disk, partition)
	_, err = runCommand("growpart", disk, partition)
	if err != nil {
		return fmt.Errorf("failed to extend partition: %w", err)
	}

	log.Printf("Running: resize2fs %s\n", mntInfo.rootDevice)
	_, err = runCommand("resize2fs", mntInfo.rootDevice)
	return err
}

func resizeGCPDisk(svc *compute.Service, disksNeedingExpansion []*compute.Disk, instMeta instanceMeta) error {
	log.Printf("disks to expand: %+v\n", disksNeedingExpansion)
	for _, disk := range disksNeedingExpansion {
		log.Printf("expanding disk %s\n", disk.Name)

		// Get expansion size from environment variable or use default
		expansionSizeGB := getExpansionSize()
		currentSizeGB := disk.SizeGb
		newSizeGB := currentSizeGB + expansionSizeGB
		log.Printf("Expanding disk by %d GB\n", expansionSizeGB)

		// Create resize request
		resizeRequest := &compute.DisksResizeRequest{
			SizeGb: newSizeGB,
		}

		// Execute resize operation
		operation, err := svc.Disks.Resize(instMeta.projectID, instMeta.zoneName, disk.Name, resizeRequest).Do()
		if err != nil {
			return fmt.Errorf("error resizing disk: %w", err)
		}
		// Extract operation name
		operationName := operation.Name
		log.Printf("Resize operation initiated (from %d GB to %d GB), operation: %s\n",
			currentSizeGB, newSizeGB, operationName)

		// Wait for the operation to complete
		log.Printf("  Waiting for operation to complete...")
		currentTime := time.Now() // Track start time

		// Poll until operation completes or times out
		for time.Since(currentTime) < diskOperationTimeout {
			// Get operation status
			op, err := svc.ZoneOperations.Get(instMeta.projectID, instMeta.zoneName, operationName).Do()
			if err != nil {
				return fmt.Errorf("error checking operation status: %w", err)
			}

			// Check if operation is done
			if op.Status == "DONE" {
				elapsed := time.Since(currentTime).Round(time.Second)
				log.Printf(" finished in %s\n", elapsed)

				if op.Error != nil {
					// Operation completed with error
					var errorMessages []string
					for _, err := range op.Error.Errors {
						errorMessages = append(errorMessages, err.Message)
					}
					return fmt.Errorf("resize failed: %s", strings.Join(errorMessages, "; "))
				}

				// Operation completed successfully
				log.Printf("Successfully resized from %d GB to %d GB\n", currentSizeGB, newSizeGB)

				break
			}

			// Wait before checking again
			time.Sleep(reCheckInterval)
		}

		// If we got here and didn't break out of the loop, we timed out
		if time.Since(currentTime) >= diskOperationTimeout {
			return fmt.Errorf("operation timed out after %s, final status unknown", diskOperationTimeout)
		}
	}
	return nil
}

func filterNeedExpansion(labeledDisks []*compute.Disk, disks []*compute.AttachedDisk, disk2device disk2deviceMapping, mntInfo mountInfo) ([]*compute.Disk, error) {
	var disksNeedingExpansion []*compute.Disk

	for _, disk := range labeledDisks {
		labelValue := disk.Labels[thresholdLabelName]
		expandThreshold, err := strconv.ParseFloat(labelValue, 64)
		if err != nil {
			return nil, fmt.Errorf("can not parse label value: %v. %w", labelValue, err)
		}

		diskName := getDiskDeviceName(disk.Name, disks)

		// check if any of the disk devices mounted as root /. Only support root partition extension for now
		for _, dev := range disk2device[diskName] {
			if dev == mntInfo.rootDevice {
				log.Printf("disk %s partition %s is mounted as root\n", disk.Name, dev)
				log.Printf("disk %s label %s=%f\n", disk.Name, thresholdLabelName, expandThreshold)
				// Get filesystem usage for root partition
				fsUsage, err := getFilesystemUsage("/")
				if err != nil {
					return nil, fmt.Errorf("error getting filesystem usage: %w", err)
				}

				if fsUsage.UsedPct > expandThreshold {
					disksNeedingExpansion = append(disksNeedingExpansion, disk)
				}

				rootInfo := fmt.Sprintf(" Usage: %.1f%% of %.1f GB, Threshold: %.1f%%",
					fsUsage.UsedPct,
					float64(fsUsage.Total)/1024/1024/1024,
					expandThreshold)

				log.Printf("- %s: %s ('%s=%s'). %s\n",
					disk.Name,
					dev,
					thresholdLabelName,
					labelValue,
					rootInfo)
			}
		}
	}
	return disksNeedingExpansion, nil
}

func logDiskInfo(disks []*compute.AttachedDisk) {
	log.Println("Disks attached to this instance:")
	for _, disk := range disks {
		diskType := "Persistent Disk"
		if disk.Type == "SCRATCH" {
			diskType = "Local SSD"
		}

		bootDisk := ""
		if disk.Boot {
			bootDisk = " (boot disk)"
		}

		log.Printf("- %s: %s %s%s\n", disk.DeviceName, disk.Source, diskType, bootDisk)
	}
}

func logMeta(instMeta instanceMeta) {
	log.Printf("Running on GCP instance\n")
	log.Printf("Instance ID: %s\n", instMeta.instanceID)
	log.Printf("Instance Name: %s\n", instMeta.instanceName)
	log.Printf("Project: %s\n", instMeta.projectID)
	log.Printf("Zone: %s\n", instMeta.zoneName)
}

func getMetadata(ctx context.Context) (instanceMeta, error) {
	// Get the instance ID with context
	instanceID, err := metadata.InstanceIDWithContext(ctx)
	if err != nil {
		return instanceMeta{}, fmt.Errorf("error retrieving instance ID: %w", err)
	}

	// Get the instance name with context
	instanceName, err := metadata.InstanceNameWithContext(ctx)
	if err != nil {
		return instanceMeta{}, fmt.Errorf("error retrieving instance name: %w", err)
	}

	// Get the project ID and zone
	projectID, err := metadata.ProjectIDWithContext(ctx)
	if err != nil {
		return instanceMeta{}, fmt.Errorf("error retrieving project ID: %w", err)
	}

	zone, err := metadata.ZoneWithContext(ctx)
	if err != nil {
		return instanceMeta{}, fmt.Errorf("error retrieving zone: %w", err)
	}
	// Extract the zone name from the full zone path
	zoneParts := strings.Split(zone, "/")
	zoneName := zoneParts[len(zoneParts)-1]
	return instanceMeta{
		instanceID:   instanceID,
		instanceName: instanceName,
		projectID:    projectID,
		zoneName:     zoneName,
	}, nil
}

// getAttachedDisks retrieves information about disks attached to the instance
func getAttachedDisks(service *compute.Service, projectID, zone, instanceName string) ([]*compute.AttachedDisk, error) {
	instance, err := service.Instances.Get(projectID, zone, instanceName).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to get instance details: %w", err)
	}

	return instance.Disks, nil
}

// getDisksWithTriggerLabel retrieves detailed information about disks that have the trigger label
func getDisksWithTriggerLabel(service *compute.Service, projectID, zone string, attachedDisks []*compute.AttachedDisk) ([]*compute.Disk, error) {
	var labeledDisks []*compute.Disk

	// Check each attached disk
	for _, attachedDisk := range attachedDisks {
		// Extract disk name from source URL
		diskNameParts := strings.Split(attachedDisk.Source, "/")
		diskName := diskNameParts[len(diskNameParts)-1]

		// Get detailed disk information
		disk, err := service.Disks.Get(projectID, zone, diskName).Do()
		if err != nil {
			return nil, fmt.Errorf("failed to get disk details for %s: %w", diskName, err)
		}

		// Check if the disk has the trigger label
		if _, ok := disk.Labels["gcp-expan-dr-trigger-usage"]; ok {
			labeledDisks = append(labeledDisks, disk)
		}
	}

	return labeledDisks, nil
}

// getDiskDeviceName returns the device name for a given disk name
func getDiskDeviceName(diskName string, attachedDisks []*compute.AttachedDisk) string {
	for _, disk := range attachedDisks {
		// Extract disk name from source URL
		diskNameParts := strings.Split(disk.Source, "/")
		sourceDiskName := diskNameParts[len(diskNameParts)-1]

		if sourceDiskName == diskName {
			return disk.DeviceName
		}
	}
	return "unknown" // If not found for some reason
}

type mount struct {
	device     string
	mountPoint string
	fsType     string
}

// mountInfo holds information about mounted filesystems
type mountInfo struct {
	rootDevice string           // The device mounted as root (/)
	mounts     map[string]mount // Map of all mount points to devices
}

// getMountInfo reads /proc/mounts to get information about mounted filesystems
func getMountInfo() (info mountInfo, err error) {
	mountFile, err := os.Open("/proc/mounts")
	if err != nil {
		return mountInfo{}, fmt.Errorf("failed to open /proc/mounts: %w", err)
	}
	defer func() {
		closeErr := mountFile.Close()
		if closeErr != nil && err == nil {
			err = fmt.Errorf("error closing /proc/mounts: %w", closeErr)
		} else if closeErr != nil {
			// Optional: combine with existing error
			err = fmt.Errorf("original error: %w, additionally failed to close file: %v", err, closeErr)
		}
	}()

	info = mountInfo{
		mounts: make(map[string]mount),
	}

	scanner := bufio.NewScanner(mountFile)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)

		if len(fields) < 3 {
			continue
		}

		device := fields[0]
		mountPoint := fields[1]
		fsType := fields[2]

		// Record the device mounted as root
		if mountPoint == "/" {
			info.rootDevice = device
		}

		info.mounts[mountPoint] = mount{device: device, mountPoint: mountPoint, fsType: fsType}
	}

	if err := scanner.Err(); err != nil {
		return mountInfo{}, fmt.Errorf("error scanning /proc/mounts: %w", err)
	}

	if info.rootDevice == "" {
		return mountInfo{}, fmt.Errorf("failed to find in /proc/mounts")
	}

	return info, nil
}

type disk2deviceMapping map[string][]string

func mapGoogleDisk2Devices(disks []*compute.AttachedDisk) (disk2deviceMapping, error) {
	result := disk2deviceMapping{}

	byIDDir := "/dev/disk/by-id"
	entries, err := os.ReadDir(byIDDir)
	if err != nil {
		return disk2deviceMapping{}, fmt.Errorf("error reading disk-by-id %s: %w", byIDDir, err)
	}

	for _, disk := range disks {
		devices, err := disk2devices(disk.DeviceName, byIDDir, entries)
		if err != nil {
			return disk2deviceMapping{}, err
		}
		result[disk.DeviceName] = devices
	}

	return result, nil
}

func disk2devices(diskName string, parentDir string, entries []os.DirEntry) ([]string, error) {
	devices := make([]string, 0)
	prefix := "google-" + diskName
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, prefix) {
			path := parentDir + "/" + name
			realPath, err := os.Readlink(path)
			if err != nil {
				return nil, fmt.Errorf("error reading link %s: %w", path, err)
			}
			absPath := filepath.Join(parentDir, realPath)
			devices = append(devices, absPath)
		}
	}
	if len(devices) == 0 {
		return nil, fmt.Errorf("can't find a single device for %s", diskName)
	}
	return devices, nil
}

// FilesystemUsage contains information about a filesystem's usage
type FilesystemUsage struct {
	Total     uint64
	Used      uint64
	Available uint64
	UsedPct   float64
}

// getFilesystemUsage retrieves the usage statistics for a filesystem
func getFilesystemUsage(path string) (FilesystemUsage, error) {
	var usage FilesystemUsage
	var stat syscall.Statfs_t

	err := syscall.Statfs(path, &stat)
	if err != nil {
		return usage, fmt.Errorf("error getting filesystem stats for %s: %w", path, err)
	}

	// Calculate total size, free space, and used space
	blockSize := uint64(stat.Bsize)
	usage.Total = stat.Blocks * blockSize
	usage.Available = stat.Bavail * blockSize
	usage.Used = usage.Total - stat.Bfree*blockSize

	// Calculate percentage used
	if usage.Total > 0 {
		usage.UsedPct = float64(usage.Used) / float64(usage.Total) * 100.0
	}

	return usage, nil
}

// getFilesystemType determines the type of filesystem at the given path
func getFilesystemType(path string, info mountInfo) (string, error) {
	for _, mount := range info.mounts {
		if mount.mountPoint == path {
			return mount.fsType, nil
		}
	}
	return "", fmt.Errorf("filesystem type not found for %s", path)
}

// extractDiskAndPartition parses a device path to extract the disk and partition number
// Example: /dev/sda1 -> disk=/dev/sda, partition=1
func extractDiskAndPartition(devicePath string) (string, string, error) {
	// This regex matches common Linux device paths like /dev/sda1, /dev/xvda1, etc.
	re := regexp.MustCompile(`^(/dev/[a-z]+)([0-9]+)$`)
	matches := re.FindStringSubmatch(devicePath)

	if len(matches) != 3 {
		return "", "", fmt.Errorf("device path does not match expected pattern: %s", devicePath)
	}

	disk := matches[1]      // /dev/sda
	partition := matches[2] // 1

	return disk, partition, nil
}

// getExpansionSize retrieves the expansion size from environment variable or returns the default
func getExpansionSize() int64 {
	expandSizeStr, exist := os.LookupEnv("GCP_EXPAN_DR_SIZE_GB")
	if exist {
		if expandSize, err := strconv.ParseInt(expandSizeStr, 10, 64); err == nil {
			return expandSize
		}
	}

	// Return default size if environment variable is not set or invalid
	log.Printf("Using default value for expantion size: %d GB\n", defaultExpandSizeGB)
	return defaultExpandSizeGB
}

// runCommand executes a shell command and returns its output
func runCommand(name string, args ...string) (string, error) {
	// Create a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()

	// Create the command with the context
	cmd := exec.CommandContext(ctx, name, args...)

	// Run the command and get the output
	output, err := cmd.CombinedOutput()

	// Check if the context deadline was exceeded
	if ctx.Err() == context.DeadlineExceeded {
		return string(output), fmt.Errorf("command timed out after 60 seconds")
	}

	if err != nil {
		return string(output), fmt.Errorf("command failed: %w - %s", err, string(output))
	}

	return string(output), nil
}
