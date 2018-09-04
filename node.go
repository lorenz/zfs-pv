/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bufio"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/golang/glog"
	zfs "github.com/lorenz/go-libzfs"
	"golang.org/x/net/context"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nodeServer struct {
	zpool  string
	prefix string
}

func newNodeServer(zfsName string) *nodeServer {
	return &nodeServer{
		zpool:  strings.Split(zfsName, "/")[0],
		prefix: zfsName,
	}
}

func (ns *nodeServer) getZFSPath(volumeID string) string {
	return path.Join(ns.prefix, volumeID)
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {

	// Check arguments
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	targetPath := req.GetTargetPath()

	readOnly := req.GetReadonly()
	volumeID := req.GetVolumeId()
	// TODO: Deal with this
	// attrib := req.GetVolumeAttributes()
	var mountflags uintptr = syscall.MS_BIND | syscall.MS_NODEV

	if readOnly {
		mountflags = mountflags | syscall.MS_RDONLY
	}

	dataset, err := zfs.DatasetOpen(ns.getZFSPath(volumeID))
	if err != nil && err.(*zfs.Error).Errno() == zfs.ENoent {
		return nil, status.Error(codes.NotFound, "Volume does not exist")
	} else if err != nil {
		return nil, status.Error(codes.Internal, "Failed to open ZFS dataset for unknown reason")
	}
	defer dataset.Close()

	isMounted, sourcePath := dataset.IsMounted()

	if !isMounted {
		err := dataset.Mount("", 0)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "ZFS internal mount failed: %v", err)
		}
		_, sourcePath = dataset.IsMounted()
	}

	mounts, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to access /proc/mounts: %v", err)
	}
	defer mounts.Close()

	scanner := bufio.NewScanner(mounts)
	for scanner.Scan() {
		line := scanner.Text()
		firstSpace := strings.IndexRune(line, ' ')
		if firstSpace == -1 {
			return nil, status.Errorf(codes.Internal, "Kernel violated /proc/mount spec", err)
		}
		secondSpace := strings.IndexRune(line[firstSpace+1:], ' ')
		if secondSpace == -1 {
			return nil, status.Errorf(codes.Internal, "Kernel violated /proc/mount spec", err)
		}
		escapedMountPath := line[firstSpace+1 : firstSpace+secondSpace+1] // TODO: Needs unescaping (octal)
		source := line[:firstSpace]

		if escapedMountPath == targetPath {
			if source != ns.getZFSPath(volumeID) {
				return nil, status.Errorf(codes.FailedPrecondition, "Target path already has %v mounted", source)
			} else {
				// TODO: Validate flag equivalence
				return &csi.NodePublishVolumeResponse{}, nil // Required volume already mounted
			}
		}
	}
	glog.V(5).Infof("Mounting %v at target %v", volumeID, targetPath)

	if err := syscall.Mount(sourcePath, targetPath, "none", mountflags, ""); err != nil {
		return nil, status.Errorf(codes.Aborted, "Failed to bind mount: %v", err)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}
	targetPath := req.GetTargetPath()
	volumeID := req.GetVolumeId()

	// Unmounting the image
	err := syscall.Unmount(targetPath, 0)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	glog.V(4).Infof("Volume %v has been unmounted from %v", volumeID, targetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			&csi.NodeServiceCapability{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_UNKNOWN,
					},
				},
			},
		},
	}, nil
}

func (ns *nodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {

	return &csi.NodeGetInfoResponse{
		NodeId: os.Getenv("KUBE_NODE_NAME"),
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{},
		},
	}, nil
}

func (ns *nodeServer) NodeGetId(ctx context.Context, req *csi.NodeGetIdRequest) (*csi.NodeGetIdResponse, error) {
	return &csi.NodeGetIdResponse{
		NodeId: os.Getenv("KUBE_NODE_NAME"),
	}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Unimplemented")
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Unimplemented")
}
