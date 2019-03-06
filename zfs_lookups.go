package main

import (
	zfs "git.dolansoft.org/lorenz/go-zfs/ioctl"
	"github.com/golang/glog"
	"golang.org/x/sys/unix"
)

func discoverClasses() map[string]string {
	classMap := make(map[string]string)
	var cursor uint64
	var name string
	var props zfs.DatasetPropsWithSource
	for {
		var err error
		name, cursor, _, props, err = zfs.DatasetListNext("", cursor)
		if err == unix.ESRCH {
			return classMap
		}
		if err != nil {
			glog.Errorf("Failed to discover storage classes: %v", err)
			return classMap
		}
		if classProp, ok := props["dolansoft-zfs:class"]; ok {
			if classProp.Source == "local" {
				classMap[classProp.Value.(string)] = name
			}
		}
	}
}
