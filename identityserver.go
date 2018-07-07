package main

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
)

type ZFSIdentityServer struct {
}

func (ids *ZFSIdentityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "csi-zfs",
		VendorVersion: "0.1-dev",
	}, nil
}

func (ids *ZFSIdentityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	// TODO: Check if zpool is up
	return &csi.ProbeResponse{Ready: true}, nil
}

func (ids *ZFSIdentityServer) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_ACCESSIBILITY_CONSTRAINTS,
					},
				},
			},
		},
	}, nil
}
