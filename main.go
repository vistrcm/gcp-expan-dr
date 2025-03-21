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
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/compute/metadata"
	"google.golang.org/api/compute/v1"
)

func main() {
	// Check if running on GCP
	if !metadata.OnGCE() {
		log.Fatal("Error: This application must be run on Google Cloud Platform")
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get the instance ID with context
	instanceID, err := metadata.InstanceIDWithContext(ctx)
	if err != nil {
		log.Fatalf("Error retrieving instance ID: %v", err)
	}

	// Get the instance name with context
	instanceName, err := metadata.InstanceNameWithContext(ctx)
	if err != nil {
		log.Fatalf("Error retrieving instance name: %v", err)
	}

	// Get the project ID and zone
	projectID, err := metadata.ProjectIDWithContext(ctx)
	if err != nil {
		log.Fatalf("Error retrieving project ID: %v", err)
	}

	zone, err := metadata.ZoneWithContext(ctx)
	if err != nil {
		log.Fatalf("Error retrieving zone: %v", err)
	}
	// Extract the zone name from the full zone path
	zoneParts := strings.Split(zone, "/")
	zoneName := zoneParts[len(zoneParts)-1]

	// Print instance information
	fmt.Printf("Running on GCP instance\n")
	fmt.Printf("Instance ID: %s\n", instanceID)
	fmt.Printf("Instance Name: %s\n", instanceName)
	fmt.Printf("Project: %s\n", projectID)
	fmt.Printf("Zone: %s\n", zoneName)

	// Get the disks attached to this instance
	disks, err := getAttachedDisks(ctx, projectID, zoneName, instanceName)
	if err != nil {
		log.Printf("Warning: Error retrieving attached disks: %v", err)
	} else {
		fmt.Println("\nDisks attached to this instance:")
		for _, disk := range disks {
			diskType := "Persistent Disk"
			if disk.Type == "SCRATCH" {
				diskType = "Local SSD"
			}

			bootDisk := ""
			if disk.Boot {
				bootDisk = " (boot disk)"
			}

			fmt.Printf("- %s: %s %s%s\n", disk.DeviceName, disk.Source, diskType, bootDisk)
		}

		// Get detailed disk information for those with the trigger label
		fmt.Println("\nDisks with gcp-expan-dr-trigger-usage label:")
		labeledDisks, err := getDisksWithTriggerLabel(ctx, projectID, zoneName, disks)
		if err != nil {
			log.Printf("Warning: Error retrieving labeled disk details: %v", err)
		} else if len(labeledDisks) == 0 {
			fmt.Println("No disks with the gcp-expan-dr-trigger-usage label found")
		} else {
			// Create compute service for disk operations
			computeService, err := compute.NewService(ctx)
			if err != nil {
				log.Printf("Warning: Error creating compute service: %v", err)
			}

			// Get mounted filesystems info once
			mountInfo, err := getMountInfo()
			if err != nil {
				log.Printf("Warning: Error getting mount information: %v", err)
			}

			// Track disks that need expansion
			var disksNeedingExpansion []string

			// Track which disks are root disks
			rootDisks := make(map[string]bool)

			for _, disk := range labeledDisks {
				labelValue := disk.Labels["gcp-expan-dr-trigger-usage"]
				deviceName := getDiskDeviceName(disk.Name, disks)

				// Check if this disk is mounted as root
				isRootDisk := false
				rootPartition := ""

				if mountInfo != nil {
					isRootDisk = isMountedAsRoot(deviceName, mountInfo)
					if isRootDisk {
						// Find which partition is actually mounted as root
						rootPartition = findRootPartition(deviceName, mountInfo)
					}
				}

				rootInfo := ""
				if isRootDisk {
					// Get filesystem usage for root partition
					fsUsage, err := getFilesystemUsage("/")
					if err != nil {
						log.Printf("Warning: Error getting filesystem usage: %v", err)
						if rootPartition != "" {
							rootInfo = fmt.Sprintf(" (ROOT FILESYSTEM - partition %s mounted as /)", rootPartition)
						} else {
							rootInfo = " (ROOT FILESYSTEM)"
						}
					} else {
						// Parse label value as percentage threshold
						labelValueFloat := 0.0
						fmt.Sscanf(labelValue, "%f", &labelValueFloat)

						// Compare usage percentage with label value
						expansionStatus := ""
						if fsUsage.UsedPct > labelValueFloat {
							expansionStatus = "NEEDS EXPANSION"
							disksNeedingExpansion = append(disksNeedingExpansion, disk.Name)
							// Store if this is a root disk
							rootDisks[disk.Name] = isRootDisk
						} else {
							expansionStatus = "No expansion needed"
						}

						// Format the usage info
						if rootPartition != "" {
							rootInfo = fmt.Sprintf(" (ROOT FILESYSTEM - partition %s mounted as /, Usage: %.1f%% of %.1f GB, Threshold: %.1f%%, Status: %s)",
								rootPartition,
								fsUsage.UsedPct,
								float64(fsUsage.Total)/1024/1024/1024,
								labelValueFloat,
								expansionStatus)
						} else {
							rootInfo = fmt.Sprintf(" (ROOT FILESYSTEM - Usage: %.1f%% of %.1f GB, Threshold: %.1f%%, Status: %s)",
								fsUsage.UsedPct,
								float64(fsUsage.Total)/1024/1024/1024,
								labelValueFloat,
								expansionStatus)
						}
					}
				}

				fmt.Printf("- %s: %s (Label value: %s)%s\n",
					disk.Name,
					deviceName,
					labelValue,
					rootInfo)
			}

			// Print the root device
			if mountInfo != nil && mountInfo.rootDevice != "" {
				fmt.Printf("\nCurrent root device: %s\n", mountInfo.rootDevice)
			}

			// Process disks that need expansion
			if len(disksNeedingExpansion) > 0 {
				fmt.Println("\n======== EXPANSION NEEDED ========")
				fmt.Println("The following disks need expansion:")

				// Track resize operations
				resizeResults := make(map[string]string)

				for _, diskName := range disksNeedingExpansion {
					fmt.Printf("- %s\n", diskName)

					if computeService != nil {
						// Get current disk info
						disk, err := computeService.Disks.Get(projectID, zoneName, diskName).Do()
						if err != nil {
							resizeResults[diskName] = fmt.Sprintf("Error getting disk info: %v", err)
							continue
						}

						// Get expansion size from environment variable or use default
						expansionSizeGB := getExpansionSize()
						currentSizeGB := disk.SizeGb
						newSizeGB := currentSizeGB + expansionSizeGB
						fmt.Printf("  Expanding disk by %d GB\n", expansionSizeGB)

						// Create resize request
						resizeRequest := &compute.DisksResizeRequest{
							SizeGb: newSizeGB,
						}

						// Execute resize operation
						operation, err := computeService.Disks.Resize(projectID, zoneName, diskName, resizeRequest).Do()
						if err != nil {
							resizeResults[diskName] = fmt.Sprintf("Error resizing disk: %v", err)
						} else {
							// Extract operation name
							operationName := operation.Name
							fmt.Printf("  Resize operation initiated (from %d GB to %d GB), operation: %s\n",
								currentSizeGB, newSizeGB, operationName)

							// Wait for the operation to complete
							fmt.Printf("  Waiting for operation to complete...")
							current_time := time.Now() // Track start time
							timeout := 5 * time.Minute // Maximum wait time for operation

							// Poll until operation completes or times out
							for time.Since(current_time) < timeout {
								// Get operation status
								op, err := computeService.ZoneOperations.Get(projectID, zoneName, operationName).Do()
								if err != nil {
									resizeResults[diskName] = fmt.Sprintf("Error checking operation status: %v", err)
									break
								}

								// Check if operation is done
								if op.Status == "DONE" {
									elapsed := time.Since(current_time).Round(time.Second)
									fmt.Printf(" finished in %s\n", elapsed)

									if op.Error != nil {
										// Operation completed with error
										errorMessages := []string{}
										for _, err := range op.Error.Errors {
											errorMessages = append(errorMessages, err.Message)
										}
										resizeResults[diskName] = fmt.Sprintf("Resize failed: %s", strings.Join(errorMessages, "; "))
									} else {
										// Operation completed successfully
										successMsg := fmt.Sprintf("Successfully resized from %d GB to %d GB", currentSizeGB, newSizeGB)
										resizeResults[diskName] = successMsg

										// Check if this is the root filesystem and is ext4
										if rootDisks[diskName] {
											// Check filesystem type
											fsType, err := getFilesystemType("/")
											if err != nil {
												fmt.Printf("\n  Warning: Could not determine root filesystem type: %v\n", err)
											} else if fsType == "ext4" {
												fmt.Printf("\n  Root filesystem is ext4, extending filesystem...\n")

												// Extend the partition and filesystem
												if rootDevice, err := getRootDevice(); err == nil && rootDevice != "" {
													// Extract disk and partition from device path
													// Example: /dev/sda1 -> disk=/dev/sda, partition=1
													disk, partition, err := extractDiskAndPartition(rootDevice)
													if err != nil {
														fmt.Printf("  Warning: Failed to parse disk and partition: %v\n", err)
														resizeResults[diskName] = successMsg + " (disk resized, but failed to parse partition info for extension)"
													} else {
														// First run growpart to extend the partition
														fmt.Printf("  Running: growpart %s %s\n", disk, partition)
														_, err := runCommand("growpart", disk, partition)
														if err != nil {
															fmt.Printf("  Warning: Failed to extend partition: %v\n", err)
															resizeResults[diskName] = successMsg + " (disk resized, but partition extension failed)"
														} else {
															// Then run resize2fs to extend the filesystem
															fmt.Printf("  Running: resize2fs %s\n", rootDevice)
															_, err := runCommand("resize2fs", rootDevice)
															if err != nil {
																fmt.Printf("  Warning: Failed to extend filesystem: %v\n", err)
																resizeResults[diskName] = successMsg + " (disk and partition resized, but filesystem extension failed)"
															} else {
																// Get updated filesystem size for reporting
																fsUsage, err := getFilesystemUsage("/")
																if err != nil {
																	fmt.Printf("  Partition and filesystem extended successfully.\n")
																	resizeResults[diskName] = successMsg + " (partition and filesystem extended successfully)"
																} else {
																	newSizeGB := float64(fsUsage.Total) / 1024 / 1024 / 1024
																	fmt.Printf("  Partition and filesystem extended successfully. New size: %.1f GB\n", newSizeGB)
																	resizeResults[diskName] = fmt.Sprintf("%s (partition and filesystem extended to %.1f GB)",
																		successMsg, newSizeGB)
																}
															}
														}
													}
												} else {
													fmt.Printf("  Warning: Could not determine root device: %v\n", err)
													resizeResults[diskName] = successMsg + " (disk resized, but couldn't identify device for filesystem extension)"
												}
											} else {
												fmt.Printf("\n  Root filesystem is %s, not extending (only ext4 supported)\n", fsType)
												resizeResults[diskName] = successMsg + fmt.Sprintf(" (%s filesystem detected, no extension attempted)", fsType)
											}
										}
									}
									break
								}

								// Print a dot to show progress
								fmt.Printf(".")

								// Wait before checking again
								time.Sleep(2 * time.Second)
							}

							// If we got here and didn't break out of the loop, we timed out
							if time.Since(current_time) >= timeout {
								fmt.Printf(" timed out after %s\n", timeout)
								resizeResults[diskName] = fmt.Sprintf("Operation timed out after %s, final status unknown", timeout)
							}
						}
					} else {
						resizeResults[diskName] = "Compute service unavailable, cannot resize"
					}
				}

				// Print resize results
				fmt.Println("\n======== RESIZE OPERATIONS ========")
				for diskName, result := range resizeResults {
					fmt.Printf("- %s: %s\n", diskName, result)
				}
				fmt.Println("==================================")
			}
		}
	}
}

// getAttachedDisks retrieves information about disks attached to the instance
func getAttachedDisks(ctx context.Context, projectID, zone, instanceName string) ([]*compute.AttachedDisk, error) {
	// Create a compute service client with default authentication
	computeService, err := compute.NewService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create compute service client: %w", err)
	}

	// Get instance details
	instance, err := computeService.Instances.Get(projectID, zone, instanceName).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to get instance details: %w", err)
	}

	return instance.Disks, nil
}

// getDisksWithTriggerLabel retrieves detailed information about disks that have the trigger label
func getDisksWithTriggerLabel(ctx context.Context, projectID, zone string, attachedDisks []*compute.AttachedDisk) ([]*compute.Disk, error) {
	// Create a compute service client
	computeService, err := compute.NewService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create compute service client: %w", err)
	}

	var labeledDisks []*compute.Disk

	// Check each attached disk
	for _, attachedDisk := range attachedDisks {
		// Extract disk name from source URL
		diskNameParts := strings.Split(attachedDisk.Source, "/")
		diskName := diskNameParts[len(diskNameParts)-1]

		// Get detailed disk information
		disk, err := computeService.Disks.Get(projectID, zone, diskName).Do()
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

// MountInfo holds information about mounted filesystems
type MountInfo struct {
	rootDevice string            // The device mounted as root (/)
	mounts     map[string]string // Map of all mount points to devices
}

// getMountInfo reads /proc/mounts to get information about mounted filesystems
func getMountInfo() (*MountInfo, error) {
	mountFile, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, fmt.Errorf("failed to open /proc/mounts: %w", err)
	}
	defer mountFile.Close()

	info := &MountInfo{
		mounts: make(map[string]string),
	}

	scanner := bufio.NewScanner(mountFile)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)

		if len(fields) < 2 {
			continue
		}

		device := fields[0]
		mountPoint := fields[1]

		// Record the device mounted as root
		if mountPoint == "/" {
			info.rootDevice = device
		}

		info.mounts[mountPoint] = device
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning /proc/mounts: %w", err)
	}

	return info, nil
}

// isMountedAsRoot checks if the given device name corresponds to the root filesystem
// by checking if any link in /dev/disk/by-id/ that starts with google-[device-name]
// points to the root device or partition
func isMountedAsRoot(deviceName string, mountInfo *MountInfo) bool {
	if mountInfo == nil || mountInfo.rootDevice == "" {
		return false
	}

	// Check if any link in /dev/disk/by-id/ that starts with google-[device-name]
	// points to the root device
	byIDDir := "/dev/disk/by-id"
	entries, err := os.ReadDir(byIDDir)
	if err != nil {
		log.Printf("Warning: Error reading %s: %v", byIDDir, err)
		return false
	}

	prefix := "google-" + deviceName
	for _, entry := range entries {
		name := entry.Name()
		// Check if the link starts with google-[device-name]
		if strings.HasPrefix(name, prefix) {
			path := byIDDir + "/" + name
			realPath, err := os.Readlink(path)
			if err != nil {
				continue
			}

			absPath := filepath.Join(byIDDir, realPath)
			// Check if this is the root device
			if absPath == mountInfo.rootDevice {
				return true
			}
		}
	}

	return false
}

// findRootPartition identifies which partition of the disk is mounted as root
func findRootPartition(deviceName string, mountInfo *MountInfo) string {
	if mountInfo == nil || mountInfo.rootDevice == "" {
		return ""
	}

	// Check the /dev/disk/by-id/ directory for links starting with google-[device-name]
	byIDDir := "/dev/disk/by-id"
	entries, err := os.ReadDir(byIDDir)
	if err != nil {
		return ""
	}

	prefix := "google-" + deviceName
	for _, entry := range entries {
		name := entry.Name()
		// Check if the link starts with google-[device-name] and contains -part for a partition
		if strings.HasPrefix(name, prefix) && strings.Contains(name, "-part") {
			path := byIDDir + "/" + name
			realPath, err := os.Readlink(path)
			if err != nil {
				continue
			}

			// If the link is relative, make it absolute
			if !strings.HasPrefix(realPath, "/") {
				realPath = "/dev/" + realPath
			}

			// If this link points to the root device, extract the partition number
			if realPath == mountInfo.rootDevice {
				parts := strings.Split(name, "-part")
				if len(parts) > 1 {
					return parts[1]
				}
				return "partition" // fallback if we can't extract the number
			}
		}
	}

	// Fallback: extract partition number from root device path if it's a basic /dev/sdXN format
	rootDevice := mountInfo.rootDevice
	if strings.HasPrefix(rootDevice, "/dev/") {
		baseDevice := strings.TrimPrefix(rootDevice, "/dev/")
		// Find where the digits start at the end
		for i := 0; i < len(baseDevice); i++ {
			if baseDevice[i] >= '0' && baseDevice[i] <= '9' {
				return baseDevice[i:]
			}
		}
	}

	return ""
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
func getFilesystemType(path string) (string, error) {
	// Read /proc/mounts to find the filesystem type
	mountFile, err := os.Open("/proc/mounts")
	if err != nil {
		return "", fmt.Errorf("failed to open /proc/mounts: %w", err)
	}
	defer mountFile.Close()

	scanner := bufio.NewScanner(mountFile)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)

		if len(fields) < 4 {
			continue
		}

		mountPoint := fields[1]
		fsType := fields[2]

		// Check if this is the mount point we're looking for
		if mountPoint == path {
			return fsType, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error scanning /proc/mounts: %w", err)
	}

	return "", fmt.Errorf("no filesystem found at path: %s", path)
}

// getRootDevice returns the device path for the root filesystem
func getRootDevice() (string, error) {
	mountInfo, err := getMountInfo()
	if err != nil {
		return "", fmt.Errorf("failed to get mount info: %w", err)
	}

	if mountInfo.rootDevice == "" {
		return "", fmt.Errorf("root device not found in mount info")
	}

	return mountInfo.rootDevice, nil
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
	// Check if environment variable is set
	envSize := os.Getenv("GCP_EXPAN_DR_SIZE_GB")
	if envSize != "" {
		// Try to parse the environment variable
		var sizeGB int64
		if _, err := fmt.Sscanf(envSize, "%d", &sizeGB); err == nil && sizeGB > 0 {
			return sizeGB
		}

		// Log a warning if the environment variable is invalid
		log.Printf("Warning: Invalid expansion size in GCP_EXPAN_DR_SIZE_GB environment variable: %s. Using default.", envSize)
	}

	// Return default size if environment variable is not set or invalid
	return 10 // Default: 10 GB
}

// runCommand executes a shell command and returns its output
func runCommand(name string, args ...string) (string, error) {
	// Create a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
