package main

import (
	zfs "git.dolansoft.org/lorenz/go-zfs/ioctl"
	"github.com/golang/glog"
)

func discoverSubtree(datasets []zfs.Dataset, classMap map[string]string) {
	for _, d := range datasets {
		path, err := d.Path()
		if err != nil {
			panic(err.Error())
		}
		p, err := d.GetUserProperty("dolansoft-zfs:class")
		if err != nil {
			panic(err)
		}
		glog.V(5).Infof("Looking at ZFS volume %v with class %v", path, p.Value)
		if p.Value != "-" && p.Source == "local" {
			if _, ok := classMap[p.Value]; ok {
				glog.Fatalf("Found duplicate class %v on %v and %v, this is unsupported", p.Value, classMap[p.Value], path)
			}
			classMap[p.Value] = path
		}
		discoverSubtree(d.Children, classMap)
	}
}

func discoverClasses() map[string]string {
	datasets, err := zfs.DatasetOpenAll()
	classMap := make(map[string]string)
	if err != nil {
		glog.Fatalf("Failed to list ZFS datasets for discovery: %v", err)
	}
	defer zfs.DatasetCloseAll(datasets)

	discoverSubtree(datasets, classMap)

	return classMap
}

func getDatasetByToken(token string) string {
	datasets, err := zfs.DatasetOpenAll()
	if err != nil {
		glog.Fatalf("Failed to list ZFS datasets to find Adoption Token: %v", err)
	}
	defer zfs.DatasetCloseAll(datasets)

	return getDatasetByTokenSubtree(token, datasets)
}

func getDatasetByTokenSubtree(token string, datasets []zfs.Dataset) string {
	for _, d := range datasets {
		path, err := d.Path()
		if err != nil {
			panic(err.Error())
		}
		p, err := d.GetUserProperty("dolansoft-zfs:adoption-token")
		if err != nil {
			panic(err)
		}

		if p.Value == token && p.Value != "-" && p.Source == "local" {
			return path
		}
		if subd := getDatasetByTokenSubtree(token, d.Children); subd != "" {
			return subd
		}
	}
	return ""
}
