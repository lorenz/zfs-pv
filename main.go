package main

import (
	"golang.org/x/sys/unix"
	"strconv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/golang/glog"
	"github.com/moby/moby/pkg/mount"
	zfs "git.dolansoft.org/lorenz/go-zfs/ioctl"
	"k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	ref "k8s.io/client-go/tools/reference"
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

func main() {
	kubeconfig := flag.String("kubeconfig", "", "absolute path to the kubeconfig file")

	flag.Parse()

	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "init":
			json.NewEncoder(os.Stdout).Encode(Init())
			return
		case "mount":
			path := os.Args[2]
			var specs map[string]string
			json.Unmarshal([]byte(os.Args[3]), &specs)
			json.NewEncoder(os.Stdout).Encode(Mount(path, specs))
			return
		case "unmount":
			path := os.Args[2]
			json.NewEncoder(os.Stdout).Encode(Unmount(path))
			return
		case "attach", "detach", "waitforattach", "isattached", "mountdevice", "unmountdevice":
			json.NewEncoder(os.Stdout).Encode(Unsupported())
			return
		}
	}

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	node, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	storageClasses := discoverClasses()
	glog.Infof("Discovered ZFS volumes: %+v", storageClasses)

	informerFactory := informers.NewSharedInformerFactory(clientset, 30*time.Second)
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims().Informer()
	pvInformer := informerFactory.Core().V1().PersistentVolumes().Informer()
	isOurs := func(pvc *v1.PersistentVolumeClaim) bool {
		return pvc.ObjectMeta.Annotations["volume.beta.kubernetes.io/storage-provisioner"] == "dolansoft.org/zfs" &&
			(pvc.ObjectMeta.Annotations["volume.kubernetes.io/selected-node"] == node || pvc.ObjectMeta.Annotations["zfs.dolansoft.org/adoption-token"] != "")
	}

	isOurPV := func(pv *v1.PersistentVolume) bool {
		return pv.ObjectMeta.Annotations["pv.kubernetes.io/provisioned-by"] == "dolansoft.org/zfs" &&
			pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values[0] == node
	}

	processPVC := func(pvc *v1.PersistentVolumeClaim) {
		if !isOurs(pvc) {
			return
		}

		if pvc.Status.Phase != "Pending" {
			// Nothing to do
			return
		}

		storageClass, err := clientset.StorageV1().StorageClasses().Get(*pvc.Spec.StorageClassName, metav1.GetOptions{})
		if err != nil {
			glog.Warningf("Failed to get StorageClass: %v", err)
			return
		}

		claimRef, err := ref.GetReference(scheme.Scheme, pvc)
		if err != nil {
			panic(err) // TODO: handling
		}

		var vol *v1.PersistentVolume

		if pvc.ObjectMeta.Annotations["zfs.dolansoft.org/adoption-token"] != "" {
			vol, err = adoptVolume(pvc, storageClasses)
			if vol == nil {
				glog.V(3).Infof("We don't have the volume to adopt")
				return // We don't have that volume, let others do their thing
			}
		} else {
			vol, err = provisionVolume(pvc, storageClasses)
		}
		if err != nil {
			panic(err) // TODO: handling
		}
		vol.Spec.ClaimRef = claimRef
		vol.Spec.NodeAffinity = &v1.VolumeNodeAffinity{
			Required: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "kubernetes.io/hostname",
								Operator: v1.NodeSelectorOpIn,
								Values:   []string{node},
							},
						},
					},
				},
			},
		}
		vol.Spec.StorageClassName = *pvc.Spec.StorageClassName
		vol.Spec.PersistentVolumeReclaimPolicy = *storageClass.ReclaimPolicy
		vol.Annotations = make(map[string]string)
		vol.Annotations["pv.kubernetes.io/provisioned-by"] = "dolansoft.org/zfs"
		if _, err = clientset.CoreV1().PersistentVolumes().Create(vol); err != nil && !apierrs.IsAlreadyExists(err) {
			glog.Warningf("Failed to create PersistentVolume: %v", err)
			return
		}
	}

	processPV := func(pv *v1.PersistentVolume) {
		if !isOurPV(pv) {
			return
		}
		if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimDelete || pv.Status.Phase != "Released" {
			return
		}
		_, err := deleteVolume(*pv, storageClasses)
		if err != nil {
			panic(err)
		}
		if err := clientset.CoreV1().PersistentVolumes().Delete(pv.Name, &metav1.DeleteOptions{}); err != nil && !apierrs.IsNotFound(err) {
			glog.Warningf("Failed to delete PersistentVolume: %v", err)
			return
		}
		return
	}

	pvcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pvc := obj.(*v1.PersistentVolumeClaim)
			processPVC(pvc)
		},
		UpdateFunc: func(oldObj interface{}, newObj interface{}) {
			newPVC := newObj.(*v1.PersistentVolumeClaim)
			oldPVC := oldObj.(*v1.PersistentVolumeClaim)
			if !isOurs(oldPVC) && isOurs(newPVC) {
				processPVC(newPVC)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// We don't care
		},
	})

	pvInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pv := obj.(*v1.PersistentVolume)
			processPV(pv)
		},
		UpdateFunc: func(oldObj interface{}, newObj interface{}) {
			pv := newObj.(*v1.PersistentVolume)
			processPV(pv)
		},
		DeleteFunc: func(obj interface{}) {
			// TODO: Handle
		},
	})

	stopper := make(chan struct{})
	defer close(stopper)
	go pvInformer.Run(stopper)
	pvcInformer.Run(stopper)
}

func provisionVolume(pvc *v1.PersistentVolumeClaim, classes map[string]string) (*v1.PersistentVolume, error) {
	props := make(zfs.DatasetProps)

	prefix, ok := classes[*pvc.Spec.StorageClassName]
	if !ok {
		return nil, fmt.Errorf("Storage class %v is not available on this host", *pvc.Spec.StorageClassName)
	}

	var datasetType zfs.ObjectType

	storageReq := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	if storageReq.IsZero() {
		return nil, fmt.Errorf("PVC is not requesting any storage, this is not supported")
	}
	capacity, ok := storageReq.AsInt64()
	if !ok {
		return nil, fmt.Errorf("PVC requesting more than 2^63 bytes of storage, this is not supported")
	}
	if *pvc.Spec.VolumeMode == v1.PersistentVolumeBlock {
		datasetType = zfs.ObjectTypeZvol
		props["volsize"] = uint64(capacity)
		if volblocksize, ok := pvc.Annotations["zfs.dolansoft.org/volblocksize"]; ok {
			if volBlockSizeInt, err := strconv.ParseUint(volblocksize, 10, 64); err != nil {
				props["volblocksize"] = volBlockSizeInt
			}
		}
	} else {
		datasetType = zfs.ObjectTypeZFS
		props["quota"] = uint64(capacity)
		if recordsize, ok := pvc.Annotations["zfs.dolansoft.org/recordsize"]; ok {
			if recordsizeInt, err := strconv.ParseUint(recordsize, 10, 64); err != nil {
				props["recordsize"] = recordsizeInt
			}
		}
		if atime, ok := pvc.Annotations["zfs.dolansoft.org/atime"]; ok {
			if atimeInt, err := strconv.ParseUint(atime, 10, 64); err != nil {
				props["atime"] = atimeInt
			}
		}
	}

	/*if compression, ok := pvc.Annotations["zfs.dolansoft.org/compression"]; ok {
		props[zfs.DatasetPropCompression] = zfs.Property{Value: compression}
	} else { // Default to lz4 because it is a better default than ZFS's off
		props[zfs.DatasetPropCompression] = zfs.Property{Value: "lz4"}
	}

	if logbias, ok := pvc.Annotations["zfs.dolansoft.org/logbias"]; ok {
		props[zfs.DatasetPropLogbias] = zfs.Property{Value: logbias}
	}*/

	props["mountpoint"] = "legacy" // Don't let a normal userspace touch this

	// Props to do:  primarycache, secondarycache, sync
	volumeID := "pvc-" + string(pvc.ObjectMeta.UID)
	identifier := path.Join(prefix, volumeID)
	glog.V(3).Infof("Creating volume %v at %v", volumeID, identifier)
	err := zfs.Create(identifier, datasetType, &props)
	_, _, _, newDataset, getErr := zfs.DatasetListNext(identifier, 0)

	if getErr != nil {
		if err != nil {
			return nil, fmt.Errorf("Failed to create ZFS volume: %v", err)
		}
		return nil, fmt.Errorf("Volume successfully created but not gettable: %v", getErr)
	}

	if err == unix.EEXIST {
		glog.V(3).Infof("Existing volume has size %v, requesting %v", newDataset["quota"].Value.(uint64), capacity)
		if newDataset["quota"].Value.(uint64) == uint64(capacity) { // TODO: Block devices
			glog.V(3).Infof("Equivalent volume %s already exists", volumeID)
			return &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: volumeID,
				},
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					Capacity: v1.ResourceList{
						v1.ResourceStorage: storageReq, // We're always giving the exact amount
					},
					PersistentVolumeSource: v1.PersistentVolumeSource{
						FlexVolume: &v1.FlexPersistentVolumeSource{
							Driver: "dolansoft.org/zfs",
							Options: map[string]string{
								"guid": strconv.FormatUint(newDataset["guid"].Value.(uint64), 10),
							},
						},
					},
				},
			}, nil
		} else {
			glog.V(2).Infof("Found conflicting volume for %s", volumeID)
			return nil, fmt.Errorf("Volume with the same name: %s but with different size already exist", pvc.Name)
		}
	} else if err != nil {
		return nil, fmt.Errorf("Volume creation failed with unexpected error: %v", err)
	}
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeID,
		},
		Spec: v1.PersistentVolumeSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Capacity: v1.ResourceList{
				v1.ResourceStorage: storageReq, // We're always giving the exact amount
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				FlexVolume: &v1.FlexPersistentVolumeSource{
					Driver: "dolansoft.org/zfs",
					Options: map[string]string{
						"guid": strconv.FormatUint(newDataset["guid"].Value.(uint64), 10),
					},
				},
			},
		},
	}, nil
}

func adoptVolume(pvc *v1.PersistentVolumeClaim, classes map[string]string) (*v1.PersistentVolume, error) {
	volumeID := "pvc-" + string(pvc.ObjectMeta.UID)
	prefix, ok := classes[*pvc.Spec.StorageClassName]
	if !ok {
		return nil, fmt.Errorf("Storage class %v is not available on this host", pvc.Spec.StorageClassName)
	}

	token := pvc.ObjectMeta.Annotations["zfs.dolansoft.org/adoption-token"]
	var cursor uint64
	var oldName string
	var props zfs.DatasetPropsWithSource
	for {	
		var err error
		oldName, cursor, _, props, err = zfs.DatasetListNext("", cursor)
		if err == unix.ESRCH {
			return nil, fmt.Errorf("Failed to find volume for adoption token %v", token)
		}
		if tokenProp, ok := props["dolansoft-zfs:adoption-token"]; ok {
			if tokenProp.Value.(string) == token {
				break
			}
		}
	}

	storageReq := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	if storageReq.IsZero() {
		return nil, fmt.Errorf("PVC is not requesting any storage, this is not supported")
	}

	if err := zfs.SetProp(oldName, map[string]interface{}{"mountpoint": "legacy"}, zfs.PropSourceDefault); err != nil {
		return nil, fmt.Errorf("Failed to set mountpoint legacy")
	}

	mounts, err := mount.GetMounts(func(m *mount.Info) (bool, bool) {
		if m.Source == oldName {
			return false, false // don't skip, keep going
		}
		return true, false // skip, keep going
	})

	for _, m := range mounts {
		if err := unix.Unmount(m.Mountpoint, 0); err != nil {
			return nil, fmt.Errorf("Failed to unmount volume %v from %v for adoption", oldName, m.Mountpoint)
		}
	}

	if err := zfs.Rename(oldName, path.Join(prefix, volumeID), false); err != nil {
		return nil, fmt.Errorf("Failed to rename ZFS dataset: %v", err)
	}

	glog.V(3).Infof("Successfully adopted dataset %v", volumeID)

	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeID,
		},
		Spec: v1.PersistentVolumeSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Capacity: v1.ResourceList{
				v1.ResourceStorage: storageReq, // We're always giving the exact amount
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				FlexVolume: &v1.FlexPersistentVolumeSource{
					Driver: "dolansoft.org/zfs",
					Options: map[string]string{
						"guid": strconv.FormatUint(props["guid"].Value.(uint64), 10),
					},
				},
			},
		},
	}, nil
}

func deleteVolume(pv v1.PersistentVolume, classes map[string]string) (bool, error) {
	volumeID := pv.Name

	prefix, ok := classes[pv.Spec.StorageClassName]
	if !ok {
		return false, fmt.Errorf("Storage class %v is not available on this host", pv.Spec.StorageClassName)
	}

	name := path.Join(prefix, volumeID)
	_, _, _, props, err := zfs.DatasetListNext(name, 0)
	if err == unix.ESRCH {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("Volume opening failed with unexpected error: %v", err)
	}

	glog.V(3).Infof("Destroying volume %s", volumeID)
	if err := zfs.Destroy(name, zfs.ObjectTypeAny, false); err != nil {
		return false, fmt.Errorf("Volume deletion failed with unexpected error: %v", err)
	}

	return true, nil
}
