package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	"gopkg.in/inf.v0"

	"github.com/golang/glog"
	zfs "github.com/lorenz/go-libzfs"
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
			pvc.ObjectMeta.Annotations["volume.kubernetes.io/selected-node"] == node
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

		vol, err := provisionVolume(pvc, storageClasses)
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

func getVolByGUIDSubtree(datasets []zfs.Dataset, guid string) string {
	for _, d := range datasets {
		path, err := d.Path()
		if err != nil {
			panic(err.Error())
		}
		p, err := d.GetProperty(zfs.DatasetPropGUID)
		if err != nil {
			panic(err)
		}
		if p.Value == guid {
			return path
		}
		if subpath := getVolByGUIDSubtree(d.Children, guid); subpath != "" {
			return subpath
		}
	}
	return ""
}

func getVolByGUID(guid string) string {
	datasets, err := zfs.DatasetOpenAll()
	if err != nil {
		glog.Fatalf("Failed to list ZFS datasets to find GUID: %v", err)
	}
	defer zfs.DatasetCloseAll(datasets)

	return getVolByGUIDSubtree(datasets, guid)
}

func provisionVolume(pvc *v1.PersistentVolumeClaim, classes map[string]string) (*v1.PersistentVolume, error) {
	props := make(map[zfs.Prop]zfs.Property)

	prefix, ok := classes[*pvc.Spec.StorageClassName]
	if !ok {
		return nil, fmt.Errorf("Storage class %v is not available on this host", *pvc.Spec.StorageClassName)
	}

	var datasetType zfs.DatasetType

	storageReq := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	if storageReq.IsZero() {
		return nil, fmt.Errorf("PVC is not requesting any storage, this is not supported")
	}
	capacity := storageReq.AsDec() // ZFS and K8s can handle volumes over 2^64 bytes in size, so don't convert to u64
	if *pvc.Spec.VolumeMode == v1.PersistentVolumeBlock {
		datasetType = zfs.DatasetTypeVolume
		props[zfs.DatasetPropVolsize] = zfs.Property{Value: capacity.String()}
		if volblocksize, ok := pvc.Annotations["zfs.dolansoft.org/volblocksize"]; ok {
			props[zfs.DatasetPropVolblocksize] = zfs.Property{Value: volblocksize}
		}
	} else {
		datasetType = zfs.DatasetTypeFilesystem
		props[zfs.DatasetPropQuota] = zfs.Property{Value: capacity.String()}
		if recordsize, ok := pvc.Annotations["zfs.dolansoft.org/recordsize"]; ok {
			props[zfs.DatasetPropRecordsize] = zfs.Property{Value: recordsize}
		}
		if atime, ok := pvc.Annotations["zfs.dolansoft.org/atime"]; ok {
			props[zfs.DatasetPropAtime] = zfs.Property{Value: atime}
		}
	}

	if compression, ok := pvc.Annotations["zfs.dolansoft.org/compression"]; ok {
		props[zfs.DatasetPropCompression] = zfs.Property{Value: compression}
	} else { // Default to lz4 because it is a better default than ZFS's off
		props[zfs.DatasetPropCompression] = zfs.Property{Value: "lz4"}
	}

	if logbias, ok := pvc.Annotations["zfs.dolansoft.org/logbias"]; ok {
		props[zfs.DatasetPropLogbias] = zfs.Property{Value: logbias}
	}

	props[zfs.DatasetPropMountpoint] = zfs.Property{Value: "legacy"} // We're managing the volume lifecyle

	// Props to do:  primarycache, secondarycache, sync
	volumeID := "pvc-" + string(pvc.ObjectMeta.UID)
	identifier := path.Join(prefix, volumeID)
	glog.V(3).Infof("Creating volume %v at %v", volumeID, identifier)
	newDataset, err := zfs.DatasetCreate(identifier, datasetType, props)
	if zerr, ok := err.(*zfs.Error); ok && zerr.Errno() == zfs.EExists {
		dataset, err := zfs.DatasetOpen(identifier)
		if err != nil {
			return nil, fmt.Errorf("Failed to get size of preexisting volume: %v", err)
		}
		defer dataset.Close()
		d := new(inf.Dec)
		glog.V(3).Infof("Existing volume has size %+v, requesting %v", dataset.Properties, capacity.String())
		if val, _ := d.SetString(dataset.Properties[zfs.DatasetPropQuota].Value); val.Cmp(capacity) == 0 { // TODO: Block devices
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
								"guid": dataset.Properties[zfs.DatasetPropGUID].Value,
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
	defer newDataset.Close()
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
						"guid": newDataset.Properties[zfs.DatasetPropGUID].Value,
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

	dataset, err := zfs.DatasetOpen(path.Join(prefix, volumeID))
	if zerr, ok := err.(*zfs.Error); ok && zerr.Errno() == zfs.ENoent {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("Volume opening failed with unexpected error: %v", err)
	}
	defer dataset.Close()

	if mounted, _ := dataset.IsMounted(); mounted {
		if err := dataset.Unmount(0); err != nil {
			return false, fmt.Errorf("Volume unmount failed with unexpected error: %v", err)
		}
	}

	glog.V(3).Infof("Destroying volume %s", volumeID)
	err = dataset.Destroy(false)
	if err != nil {
		return false, fmt.Errorf("Volume deletion failed with unexpected error: %v", err)
	}

	return true, nil
}
