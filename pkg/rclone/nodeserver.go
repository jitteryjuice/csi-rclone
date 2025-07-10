package rclone

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume/util"

	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
)

type mountContext struct {
	rcPort int
}

type nodeServer struct {
	Driver *Driver
	*csicommon.DefaultNodeServer
	mounter      *mount.SafeFormatAndMount
	mountContext map[string]*mountContext
	mu           sync.RWMutex
}

func (ns *nodeServer) getMountContext(targetPath string) *mountContext {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	if mc, ok := ns.mountContext[targetPath]; ok {
		return mc
	}
	return &mountContext{}
}

func (ns *nodeServer) setMountContext(targetPath string, mc *mountContext) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	// create a new mount context
	if ns.mountContext == nil {
		ns.mountContext = make(map[string]*mountContext)
	}
	ns.mountContext[targetPath] = mc
}

func (ns *nodeServer) deleteMountContext(targetPath string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	delete(ns.mountContext, targetPath)
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	glog.V(4).Infof("NodePublishVolume: called with args %+v", *req)

	targetPath := req.GetTargetPath()

	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	if !notMnt {
		// testing original mount point, make sure the mount link is valid
		if _, err := ioutil.ReadDir(targetPath); err == nil {
			glog.V(4).Infof("already mounted to target %s", targetPath)
			return &csi.NodePublishVolumeResponse{}, nil
		}
		// todo: mount link is invalid, now unmount and remount later (built-in functionality)
		glog.Warningf("ReadDir %s failed with %v, unmount this directory", targetPath, err)

		ns.mounter = &mount.SafeFormatAndMount{
			Interface: mount.New(""),
			Exec:      mount.NewOsExec(),
		}

		if err := ns.mounter.Unmount(targetPath); err != nil {
			glog.Errorf("Unmount directory %s failed with %v", targetPath, err)
			return nil, err
		}
	}

	mountOptions := req.GetVolumeCapability().GetMount().GetMountFlags()
	if req.GetReadonly() {
		mountOptions = append(mountOptions, "ro")
	}

	remote, remotePath, configData, flags, e := extractFlags(req.GetVolumeContext())
	if e != nil {
		glog.Warningf("storage parameter error: %s", e)
		return nil, e
	}

	rcPort, e := Mount(remote, remotePath, targetPath, configData, flags)
	if e != nil {
		if os.IsPermission(e) {
			return nil, status.Error(codes.PermissionDenied, e.Error())
		}
		if strings.Contains(e.Error(), "invalid argument") {
			return nil, status.Error(codes.InvalidArgument, e.Error())
		}
		return nil, status.Error(codes.Internal, e.Error())
	}

	// Save the mount context
	ns.setMountContext(targetPath, &mountContext{
		rcPort: rcPort,
	})

	return &csi.NodePublishVolumeResponse{}, nil
}

func extractFlags(volumeContext map[string]string) (string, string, string, map[string]string, error) {

	// Empty argument list
	flags := make(map[string]string)

	// Load default connection settings from secret
	var secret *v1.Secret

	if secretName, ok := volumeContext["secretName"]; ok {
		// Load the secret that the PV spec defines
		var e error
		secret, e = getSecret(secretName)
		if e != nil {
			// if the user explicitly requested a secret and there is an error fetching it, bail with an error
			return "", "", "", nil, e
		}
	} else {
		// use rclone-secret as the default secret if none was defined
		secret, _ = getSecret("rclone-secret")
	}

	// Secret values are default, gets merged and overriden by corresponding PV values
	if secret != nil && secret.Data != nil && len(secret.Data) > 0 {
		// Needs byte to string casting for map values
		for k, v := range secret.Data {
			flags[k] = string(v)
		}
	} else {
		glog.Infof("No csi-rclone connection defaults secret found.")
	}

	if len(volumeContext) > 0 {
		for k, v := range volumeContext {
			flags[k] = v
		}
	}

	if e := validateFlags(flags); e != nil {
		return "", "", "", flags, e
	}

	remote := flags["remote"]
	remotePath := flags["remotePath"]

	if remotePathSuffix, ok := flags["remotePathSuffix"]; ok {
		remotePath = remotePath + remotePathSuffix
		delete(flags, "remotePathSuffix")
	}

	configData := ""
	ok := false

	if configData, ok = flags["configData"]; ok {
		delete(flags, "configData")
	}

	delete(flags, "remote")
	delete(flags, "remotePath")
	delete(flags, "secretName")

	return remote, remotePath, configData, flags, nil
}

// https://rclone.org/rc/#core-stats
type rcCoreStatsResponse struct {
	// an array of currently active file transfers
	Transferring map[string]interface{} `json:"transferring"`
}

// https://rclone.org/rc/#vfs-stats
type rcVfsStatsResponse struct {
	DiskCache struct {
		UploadsInProgress int64 `json:"uploadsInProgress"`
		UploadsQueued     int64 `json:"uploadsQueued"`
	} `json:"diskCache"`
}

// RcloneRPC is a helper function to call rclone rc server
func RcloneRPC(host string, method string, input string) (output string, err error) {
	url := fmt.Sprintf("http://%s/%s", host, method)

	// Create a POST request to API
	req, err := http.NewRequest("POST", url, strings.NewReader(input))
	if err != nil {
		return "", fmt.Errorf("cannot create HTTP request: %v", err)
	}

	// Set the content type to JSON
	req.Header.Set("Content-Type", "application/json")

	// Create a new HTTP client
	client := &http.Client{}

	// Send the request via the client
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("cannot send HTTP request: %v", err)
	}

	// Close the response body on function exit
	defer resp.Body.Close()

	// Read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("cannot read HTTP response: %v", err)
	}

	// Return the response body as a string
	return string(body), nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {

	targetPath := req.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "NodeUnpublishVolume Target Path must be provided")
	}

	mountContext := ns.getMountContext(targetPath)
	rcPort := mountContext.rcPort

	if rcPort != 0 {
		// Connect to rclone rpc server and query the operation status
		// If the rclone process is still running, wait for it to finish cache sync
		// If the rclone process is not running, proceed to volume unmount
		// check the state of the rclone process until it finishes the cache sync
		// Hard timeout is 1 hour
		copyTimeout := time.Now().Add(1 * time.Hour)
		for copyTimeout.After(time.Now()) {

			// Try to load https://localhost:5572/core/stats and parse the JSON response
			out, err := RcloneRPC(fmt.Sprintf("localhost:%s", strconv.Itoa(rcPort)), "core/stats", "{}")
			if err == nil {
				var coreStats rcCoreStatsResponse
				err = json.Unmarshal([]byte(out), &coreStats)
				if err == nil {
					if len(coreStats.Transferring) > 0 {
						time.Sleep(5 * time.Second)
						continue
					}
				}

			}

			// Try to load https://localhost:5572/vfs/stats and parse the JSON response
			out, err = RcloneRPC(fmt.Sprintf("localhost:%s", strconv.Itoa(rcPort)), "vfs/stats", "{}")
			if err == nil {
				var vfsStats rcVfsStatsResponse
				err = json.Unmarshal([]byte(out), &vfsStats)
				if err == nil {
					if vfsStats.DiskCache.UploadsInProgress > 0 || vfsStats.DiskCache.UploadsQueued > 0 {
						time.Sleep(5 * time.Second)
						continue
					}
				}
			}

			// proceed to volume unmount
			break
		}

		// Remove VFS cache
		os.RemoveAll("/tmp/rclone-vfs-cache/" + targetPath)
	}

	// Remove mount context
	ns.deleteMountContext(targetPath)

	m := mount.New("")

	notMnt, err := m.IsLikelyNotMountPoint(targetPath)
	if err != nil && !mount.IsCorruptedMnt(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if notMnt && !mount.IsCorruptedMnt(err) {
		glog.V(4).Infof("Volume not mounted")

	} else {
		err = util.UnmountPath(req.GetTargetPath(), m)
		if err != nil {
			glog.V(4).Infof("Error while unmounting path: %s", err)
			// This will exit and fail the NodeUnpublishVolume making it to retry unmount on the next api schedule trigger.
			// Since we mount the volume with allow-non-empty now, we could skip this one too.
			return nil, status.Error(codes.Internal, err.Error())
		}

		glog.V(4).Infof("Volume %s unmounted successfully", req.VolumeId)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return &csi.NodeStageVolumeResponse{}, nil
}

func validateFlags(flags map[string]string) error {
	if _, ok := flags["remote"]; !ok {
		return status.Errorf(codes.InvalidArgument, "missing volume context value: remote")
	}
	if _, ok := flags["remotePath"]; !ok {
		return status.Errorf(codes.InvalidArgument, "missing volume context value: remotePath")
	}
	return nil
}

func getSecret(secretName string) (*v1.Secret, error) {
	clientset, e := GetK8sClient()
	if e != nil {
		return nil, status.Errorf(codes.Internal, "can not create kubernetes client: %s", e)
	}

	kubeconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{},
	)

	namespace, _, err := kubeconfig.Namespace()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "can't get current namespace for secret %s, error %s", secretName, err)
	}

	glog.V(4).Infof("Loading csi-rclone connection defaults from secret %s/%s", namespace, secretName)

	secret, e := clientset.CoreV1().
		Secrets(namespace).
		Get(secretName, metav1.GetOptions{})

	if e != nil {
		return nil, status.Errorf(codes.Internal, "can't load csi-rclone settings from secret %s: %s", secretName, e)
	}

	return secret, nil
}

func flagToEnvName(flag string) string {
	// To find the name of the environment variable, first, take the long option name, strip the leading --, change - to _, make upper case and prepend RCLONE_.
	flag = strings.TrimPrefix(flag, "--") // we dont pass prefixed args, but strictly this is the algorithm
	flag = strings.ReplaceAll(flag, "-", "_")
	flag = strings.ToUpper(flag)
	return fmt.Sprintf("RCLONE_%s", flag)
}

// Credit: https://gist.github.com/sevkin/96bdae9274465b2d09191384f86ef39d
func getFreePort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return 0, err
}

// Function to get rclone rc details
// If the user provides the rc-addr flag, it will use that address
// If the user does not provide the rc-addr flag, it will find a free port
// and use localhost as the host
func getRcDetails(flags map[string]string) (host string, port int, err error) {

	rcHost := "localhost"
	var rcPort int
	// Find a free port for rclone rc
	rcPort, err = getFreePort()
	if err != nil {
		return "", 0, err
	}
	// Check if the user has provided the rc address
	if rcAddr, ok := flags["rc-addr"]; ok {
		// Parse the rcAddr
		glog.V(4).Infof("processing user provide rc-addr: %s", rcAddr)
		host, portStr, err := net.SplitHostPort(rcAddr)
		if err != nil {
			return "", 0, fmt.Errorf("invalid rc-addr format: %s, error: %v", rcAddr, err)
		}
		if host == "" {
			host = "0.0.0.0" // Default to all interfaces if no host is provided
		}
		rcHost = host
		if portStr != "" {
			rcPort, err = strconv.Atoi(portStr)
			if err != nil {
				return "", 0, fmt.Errorf("invalid rc-addr port: %s, error: %v", portStr, err)
			}
		}
	}
	glog.V(4).Infof("using rclone rc with %s:%d", rcHost, rcPort)
	return rcHost, rcPort, nil
}

// Mount routine.
func Mount(remote string, remotePath string, targetPath string, configData string, flags map[string]string) (rcPort int, err error) {
	mountCmd := "rclone"
	mountArgs := []string{}

	// Retrieve rclone rc details
	// If rc-addr is not set, use a free port
	// If rc-addr is set, use the provided host and port
	rcHost, rcPort, err := getRcDetails(flags)
	if err != nil {
		return 0, fmt.Errorf("failed to get rclone rc details: %v", err)
	}

	defaultFlags := map[string]string{}
	defaultFlags["cache-info-age"] = "72h"
	defaultFlags["cache-chunk-clean-interval"] = "15m"
	defaultFlags["dir-cache-time"] = "5s"
	defaultFlags["vfs-cache-mode"] = "writes"
	defaultFlags["cache-dir"] = "/tmp/rclone-vfs-cache/" + targetPath
	defaultFlags["allow-non-empty"] = "true"
	defaultFlags["allow-other"] = "true"
	defaultFlags["rc"] = ""
	defaultFlags["rc-addr"] = fmt.Sprintf("%s:%d", rcHost, rcPort)

	remoteWithPath := fmt.Sprintf(":%s:%s", remote, remotePath)

	if strings.Contains(configData, "["+remote+"]") {
		remoteWithPath = fmt.Sprintf("%s:%s", remote, remotePath)
		glog.V(4).Infof("remote %s found in configData, remoteWithPath set to %s", remote, remoteWithPath)
	}

	// rclone mount remote:path /path/to/mountpoint [flags]
	mountArgs = append(
		mountArgs,
		"mount",
		remoteWithPath,
		targetPath,
		"--daemon",
		"--daemon-wait=0",
	)

	// If a custom flag configData is defined,
	// create a temporary file, fill it with  configData content,
	// and run rclone with --config <tmpfile> flag
	if configData != "" {
		configFile, err := os.CreateTemp("", "rclone.conf")
		if err != nil {
			return 0, err
		}

		// Normally, a defer os.Remove(configFile.Name()) should be placed here.
		// However, due to a rclone mount --daemon flag, rclone forks and creates a race condition
		// with this nodeplugin proceess. As a result, the config file gets deleted
		// before it's reread by a forked process.
		if _, err := configFile.Write([]byte(configData)); err != nil {
			return 0, err
		}
		if err := configFile.Close(); err != nil {
			return 0, err
		}

		mountArgs = append(mountArgs, "--config", configFile.Name())
	} else {
		// Disable "config not found" notice
		mountArgs = append(mountArgs, "--config=''")
	}

	// Add default flags
	for k, v := range defaultFlags {
		// Exclude overriden flags
		if _, ok := flags[k]; !ok {
			// env = append(env, fmt.Sprintf("%s=%s", flagToEnvName(k), v))
			if v == "" {
				mountArgs = append(mountArgs, fmt.Sprintf("--%s", k))
			} else {
				mountArgs = append(mountArgs, fmt.Sprintf("--%s=%s", k, v))
			}
		}
	}

	// Add user supplied flags
	for k, v := range flags {
		// env = append(env, fmt.Sprintf("%s=%s", flagToEnvName(k), v))
		if v == "" {
			mountArgs = append(mountArgs, fmt.Sprintf("--%s", k))
		} else {
			mountArgs = append(mountArgs, fmt.Sprintf("--%s=%s", k, v))
		}
	}

	// create target, os.Mkdirall is noop if it exists
	err = os.MkdirAll(targetPath, 0750)
	if err != nil {
		return 0, err
	}

	glog.V(4).Infof("executing mount command cmd=%s, remote=%s, targetpath=%s", mountCmd, remoteWithPath, targetPath)
	glog.V(4).Infof("mountArgs: %v", mountArgs)

	cmd := exec.Command(mountCmd, mountArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("mounting failed: %v cmd: '%s' remote: '%s' targetpath: %s output: %q",
			err, mountCmd, remoteWithPath, targetPath, string(out))
	}

	return rcPort, nil
}
