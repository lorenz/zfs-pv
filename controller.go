package main

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/lorenz/go-libzfs"
)

func shouldEscape(c byte) bool {
	if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' {
		return false
	}
	if c == '_' || c == '-' || c == '.' {
		return false
	}
	return true
}

func zfsDatasetEscape(s string) string {
	hexCount := 0
	for i := 0; i < len(s); i++ {

		if shouldEscape(s[i]) {
			hexCount++
		}
	}

	if hexCount == 0 {
		return s
	}

	t := make([]byte, len(s)+2*hexCount)
	j := 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case shouldEscape(c):
			t[j] = ':'
			t[j+1] = "0123456789ABCDEF"[c>>4]
			t[j+2] = "0123456789ABCDEF"[c&15]
			j += 3
		default:
			t[j] = s[i]
			j++
		}
	}
	return string(t)
}

func (cs *controllerServer) getZFSPath(volumeID string) string {
	return path.Join(cs.prefix, volumeID)
}

type controllerServer struct {
	zpool  string
	prefix string
}

func newControllerServer(zfsName string) *controllerServer {
	return &controllerServer{
		zpool:  strings.Split(zfsName, "/")[0],
		prefix: zfsName,
	}
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	// Check arguments
	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	props := make(map[zfs.Prop]zfs.Property)

	var datasetType zfs.DatasetType

	var capacity int64
	if req.CapacityRange == nil {
		capacity = 1024 * 1024 * 1024 // 1GiB
	} else if req.CapacityRange.LimitBytes > 0 {
		capacity = req.CapacityRange.LimitBytes
	} else if req.CapacityRange.RequiredBytes > 0 {
		capacity = req.CapacityRange.RequiredBytes
	} else {
		capacity = 1024 * 1024 * 1024 // 1GiB
	}

	for _, capability := range req.GetVolumeCapabilities() {
		if capability.AccessMode.Mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
			capability.AccessMode.Mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY {
			return nil, status.Error(codes.InvalidArgument, "Only capable of node-local access modes")
		}
		switch capability.AccessType.(type) { // TODO: Handle stupid case (both block and volume access)
		case *csi.VolumeCapability_Mount:
			datasetType = zfs.DatasetTypeFilesystem
			props[zfs.DatasetPropQuota] = zfs.Property{Value: strconv.FormatInt(capacity, 10)}
			if recordsize, ok := req.Parameters["recordsize"]; ok {
				props[zfs.DatasetPropRecordsize] = zfs.Property{Value: recordsize}
			}
			if atime, ok := req.Parameters["atime"]; ok {
				props[zfs.DatasetPropAtime] = zfs.Property{Value: atime}
			}
		case *csi.VolumeCapability_Block:
			datasetType = zfs.DatasetTypeVolume
			props[zfs.DatasetPropVolsize] = zfs.Property{Value: strconv.FormatInt(capacity, 10)}
			if volblocksize, ok := req.Parameters["volblocksize"]; ok {
				props[zfs.DatasetPropVolblocksize] = zfs.Property{Value: volblocksize}
			}
		}
	}

	if compression, ok := req.Parameters["compression"]; ok {
		props[zfs.DatasetPropCompression] = zfs.Property{Value: compression}
	} else { // Default to lz4 because it is a better default than ZFS's off
		props[zfs.DatasetPropCompression] = zfs.Property{Value: "lz4"}
	}

	if logbias, ok := req.Parameters["logbias"]; ok {
		props[zfs.DatasetPropLogbias] = zfs.Property{Value: logbias}
	}

	// Props to do:  primarycache, secondarycache, sync
	volumeID := zfsDatasetEscape(req.Name)
	identifier := cs.getZFSPath(volumeID)
	glog.V(4).Infof("Creating volume %s", volumeID)
	newDataset, err := zfs.DatasetCreate(identifier, datasetType, props)
	if zerr, ok := err.(*zfs.Error); ok && zerr.Errno() == zfs.EExists {
		dataset, err := zfs.DatasetOpen(identifier)
		if err != nil {
			return nil, status.Error(codes.Aborted, fmt.Sprintf("Failed to get size of preexisting volume: %v", err))
		}
		defer dataset.Close()
		if val, _ := strconv.ParseInt(dataset.Properties[zfs.DatasetPropQuota].Value, 10, 64); val == capacity { // TODO: Block devices
			glog.V(3).Infof("Equivalent volume %s already exists", volumeID)
			return &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					Id:            volumeID,
					CapacityBytes: int64(capacity),
					Attributes:    req.GetParameters(),
				},
			}, nil
		} else {
			glog.V(2).Infof("Found conflicting volume for %s", volumeID)
			return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("Volume with the same name: %s but with different size already exist", req.GetName()))
		}
	} else if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Volume creation failed with unexpected error: %v", err))
	}
	newDataset.Close()

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			Id:            volumeID,
			CapacityBytes: int64(capacity),
			Attributes:    req.GetParameters(),
		},
	}, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	volumeID := req.VolumeId
	dataset, err := zfs.DatasetOpen(cs.getZFSPath(volumeID))
	if zerr, ok := err.(*zfs.Error); ok && zerr.Errno() == zfs.ENoent {
		return &csi.DeleteVolumeResponse{}, nil
	} else if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Volume opening failed with unexpected error: %v", err))
	}
	defer dataset.Close()

	if mounted, _ := dataset.IsMounted(); mounted {
		if err := dataset.Unmount(0); err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("Volume unmount failed with unexpected error: %v", err))
		}
	}

	glog.V(4).Infof("deleting volume %s", volumeID)
	err = dataset.Destroy(false)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Volume deletion failed with unexpected error: %v", err))
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities missing in request")
	}

	volumeID := req.VolumeId

	dataset, err := zfs.DatasetOpen(cs.getZFSPath(volumeID))
	if zerr, ok := err.(*zfs.Error); ok && zerr.Errno() == zfs.ENoent {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume %v doesn't exist", volumeID))
	} else if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Volume opening failed with unexpected error: %v", err))
	}
	defer dataset.Close()

	for _, cap := range req.VolumeCapabilities {
		if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
			cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY {
			return &csi.ValidateVolumeCapabilitiesResponse{Supported: false, Message: "ZFS doesn't support any multi-node access modes"}, nil
		}
		switch cap.AccessType.(type) {
		case *csi.VolumeCapability_Mount:
			if dataset.Properties[zfs.DatasetPropType].Value != "filesystem" {
				return &csi.ValidateVolumeCapabilitiesResponse{Supported: false, Message: "Cannot access block device as filesystem"}, nil
			}
		case *csi.VolumeCapability_Block:
			if dataset.Properties[zfs.DatasetPropType].Value != "volume" {
				return &csi.ValidateVolumeCapabilitiesResponse{Supported: false, Message: "Cannot access filesystem as block device"}, nil
			}
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{Supported: true, Message: ""}, nil
}

func (cs *controllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME}}},
		},
	}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Unimplemented")
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Unimplemented")
}

func (cs *controllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Unimplemented")
}

func (cs *controllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Unimplemented")
}

func (cs *controllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Unimplemented")
}

func (cs *controllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Unimplemented")
}

func (cs *controllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Unimplemented")
}
