package main

import (
	zfs "git.dolansoft.org/lorenz/go-zfs/ioctl"
	"github.com/golang/glog"
	"golang.org/x/sys/unix"
)

func discoverClasses() map[string]string {
	classMap := make(map[string]string)
	_, err := listAndFilterDatasets(func(name string, props zfs.DatasetPropsWithSource) (bool, bool) {
		if classProp, ok := props["dolansoft-zfs:class"]; ok {
			if classProp.Source == name {
				classMap[classProp.Value.(string)] = name
			}
		}
		return true, false // Don't add anything to the result set, continue always
	})
	if err != nil {
		glog.Errorf("Failed to get classes: %v", err)
		// TODO: Maybe fail hard
	}
	return classMap
}

func listAndFilterDatasets(filter func(string, zfs.DatasetPropsWithSource) (bool, bool)) ([]string, error) {
	var names []string
	pools, err := zfs.PoolConfigs()
	if err != nil {
		return names, err
	}
pool:
	for pool := range pools {
		var name string
		var props zfs.DatasetPropsWithSource
		props, err = zfs.ObjsetStats(pool) // Root dataset
		cont, add := filter(pool, props)
		if add {
			names = append(names, pool)
		}
		if !cont {
			break pool
		}
		var recurseList func(string) (error, bool)
		recurseList = func(prefix string) (error, bool) {
			var cursor uint64
			for {
				var err error
				name, cursor, _, props, err = zfs.DatasetListNext(prefix, cursor)
				if err == unix.ESRCH {
					break
				}
				if err != nil {
					return err, false
				}
				cont, add := filter(name, props)
				if add {
					names = append(names, name)
				}
				if !cont {
					return nil, false
				}
				err, cont = recurseList(name)
				if err != nil {
					return err, false
				}
				if !cont {
					return nil, false
				}
			}
			return nil, true
		}
		err, _ := recurseList(pool)
		if err != nil {
			return names, err
		}
	}
	return names, nil
}
