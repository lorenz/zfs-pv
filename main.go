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
	"flag"
	"net"
	"os"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

func main() {
	flag.Parse()
	listener, err := net.Listen("unix", os.Getenv("CSI_ENDPOINT"))
	if err != nil {
		glog.Fatalf("Failed to listen: %v", err)
	}

	opts := []grpc.ServerOption{}
	server := grpc.NewServer(opts...)

	csi.RegisterIdentityServer(server, &identityServer{})
	csi.RegisterControllerServer(server, &controllerServer{})
	csi.RegisterNodeServer(server, &nodeServer{})

	glog.Infof("Listening for connections on address: %#v", listener.Addr())

	server.Serve(listener)
}
