package main

import (
	"context"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/lorenz/go-libzfs"
)

type identityServer struct {
	zpool string
}

func newIdentityServer(zfsName string) *identityServer {
	return &identityServer{
		zpool: strings.Split(zfsName, "/")[0],
	}
}

func (ids *identityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "zfs-csi",
		VendorVersion: "0.1-dev",
	}, nil
}

func (ids *identityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	pool, err := zfs.PoolOpen(ids.zpool)
	if err != nil {
		return &csi.ProbeResponse{Ready: &wrappers.BoolValue{Value: false}}, nil
	}
	pool.Close()
	return &csi.ProbeResponse{Ready: &wrappers.BoolValue{Value: true}}, nil
}

func (ids *identityServer) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
			/*{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_ACCESSIBILITY_CONSTRAINTS,
					},
				},
			},*/
		},
	}, nil
}
