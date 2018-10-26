package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/golang/glog"
	zfs "github.com/lorenz/go-libzfs"
)

// From k8s.io/pkg/volume/flexvolume/driver-call.go

type DriverStatus struct {
	// Status of the callout. One of "Success", "Failure" or "Not supported".
	Status string `json:"status"`
	// Reason for success/failure.
	Message string `json:"message,omitempty"`
	// Path to the device attached. This field is valid only for attach calls.
	// ie: /dev/sdx
	DevicePath string `json:"device,omitempty"`
	// Cluster wide unique name of the volume.
	VolumeName string `json:"volumeName,omitempty"`
	// Represents volume is attached on the node
	Attached bool `json:"attached,omitempty"`
	// Returns capabilities of the driver.
	// By default we assume all the capabilities are supported.
	// If the plugin does not support a capability, it can return false for that capability.
	Capabilities *DriverCapabilities `json:",omitempty"`
}

// DriverCapabilities represents what driver can do
type DriverCapabilities struct {
	Attach          bool `json:"attach"`
	SELinuxRelabel  bool `json:"selinuxRelabel"`
	SupportsMetrics bool `json:"supportsMetrics"`
	FSGroup         bool `json:"fsGroup"`
}

func Init() *DriverStatus {
	return &DriverStatus{
		Status: "Success",
		Capabilities: &DriverCapabilities{
			Attach:          false,
			SELinuxRelabel:  false,
			SupportsMetrics: false,
			FSGroup:         false,
		},
	}
}

func Mount(path string, specs map[string]string) *DriverStatus {
	zvol := getVolByGUID(specs["guid"])
	if zvol == "" {
		return &DriverStatus{
			Status:  "Failure",
			Message: fmt.Sprintf("Failed to find volume for GUID %v", specs["guid"]),
		}
	}
	var mountflags uintptr = syscall.MS_NODEV | syscall.MS_NOSUID

	if specs["kubernetes.io/readwrite"] == "ro" {
		mountflags = mountflags | syscall.MS_RDONLY
	}

	dataset, err := zfs.DatasetOpen(zvol)
	if err != nil {
		return &DriverStatus{
			Status:  "Failure",
			Message: fmt.Sprintf("Failed to find volume for GUID %v", specs["guid"]),
		}
	}
	defer dataset.Close()

	// TODO: This is technically a race (mount check is not atomic with mount), needs some kind of locking

	mounts, err := os.Open("/proc/mounts")
	if err != nil {
		return &DriverStatus{
			Status:  "Failure",
			Message: fmt.Sprintf("Failed to access /proc/mounts: %v", err),
		}
	}
	defer mounts.Close()

	scanner := bufio.NewScanner(mounts)
	for scanner.Scan() {
		line := scanner.Text()
		firstSpace := strings.IndexRune(line, ' ')
		if firstSpace == -1 {
			glog.Errorf("Kernel violated /proc/mount spec, didn't find a space")
			return &DriverStatus{
				Status:  "Failure",
				Message: fmt.Sprintf("Kernel violated /proc/mount spec"),
			}
		}
		secondSpace := strings.IndexRune(line[firstSpace+1:], ' ')
		if secondSpace == -1 {
			glog.Errorf("Kernel violated /proc/mount spec, didn't find a second space")
			return &DriverStatus{
				Status:  "Failure",
				Message: fmt.Sprintf("Kernel violated /proc/mount spec"),
			}
		}
		escapedMountPath := line[firstSpace+1 : firstSpace+secondSpace+1] // TODO: Needs unescaping (octal)
		source := line[:firstSpace]

		if escapedMountPath == path {
			if source != zvol {
				glog.Warningf("Target path for mount of %v already has %v mounted", zvol, source)
				return &DriverStatus{
					Status:  "Failure",
					Message: fmt.Sprintf("Target path already has %v mounted", source),
				}
			} else {
				// TODO: Validate flag equivalence
				return &DriverStatus{
					Status:  "Success",
					Message: fmt.Sprintf("Volume was already mounted, doing nothing"),
				}
			}
		}
	}
	glog.V(3).Infof("Mounting %v at target %v", zvol, path)

	if err := syscall.Mount(zvol, path, "zfs", mountflags, ""); err != nil {
		return &DriverStatus{
			Status:  "Failure",
			Message: fmt.Sprintf("Failed to mount ZFS volume %v: %v", zvol, err),
		}
	}

	return &DriverStatus{
		Status: "Success",
	}
}

func Unmount(path string) *DriverStatus {
	// Unmounting the image
	err := syscall.Unmount(path, 0)
	if err != nil {
		return &DriverStatus{
			Status:  "Failure",
			Message: fmt.Sprintf("Failed to unmount ZFS volume %v: %v", path, err),
		}
	}
	glog.V(3).Infof("Volume has been unmounted from %v", path)

	return &DriverStatus{
		Status: "Success",
	}
}

func Unsupported() *DriverStatus {
	return &DriverStatus{
		Status: "Not Supported",
	}
}
