package main

import (
	"context"

	"github.com/bicomsystems/go-libzfs"
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/protobuf/ptypes/wrappers"
)

type identityServer struct {
}

func (ids *identityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "zfs-csi",
		VendorVersion: "0.1-dev",
	}, nil
}

func (ids *identityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	dataset, err := zfs.DatasetOpen("TESTPOOL")
	if err != nil {
		return &csi.ProbeResponse{Ready: &wrappers.BoolValue{Value: false}}, nil
	}
	dataset.Close()
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
