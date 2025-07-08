
# CSI rclone mount plugin

This project implements Container Storage Interface (CSI) plugin that allows using [rclone mount](https://rclone.org/) as storage backend. Rclone mount points and [parameters](https://rclone.org/commands/rclone_mount/) can be configured using Secret or PersistentVolume volumeAttibutes. 

## Kubernetes cluster compatability
Works (tested):
- `deploy/kubernetes/1.33`: K8S>= 1.30.x External provisioner requires kubernetes [1.20](https://github.com/kubernetes-csi/external-provisioner?tab=readme-ov-file#compatibility)+.

## Installing CSI driver to kubernetes cluster
TLDR: `kubectl apply -f deploy/kubernetes/1.20`

1. Set up storage backend. You can use [Minio](https://min.io/), Amazon S3 compatible cloud storage service.
i.e (heads up - minio setup example is severly outdated). 
```
helm upgrade --install --create-namespace --namespace minio minio minio/minio --version 6.0.5 --set resources.requests.memory=512Mi --set secretKey=SECRET_ACCESS_KEY --set accessKey=ACCESS_KEY_ID
```

2. Configure defaults by pushing secret to kube-system namespace. This is optional if you will always define `volumeAttributes` in PersistentVolume.

```
apiVersion: v1
kind: Secret
metadata:
  name: rclone-secret
type: Opaque
stringData:
  remote: "s3"
  remotePath: "projectname"
  s3-provider: "Minio"
  s3-endpoint: "http://minio.minio:9000"
  s3-access-key-id: "ACCESS_KEY_ID"
  s3-secret-access-key: "SECRET_ACCESS_KEY"
```

Alternatively, you may specify rclone configuration file directly in the secret under `configData` field.

```
apiVersion: v1
kind: Secret
metadata:
  name: rclone-secret
type: Opaque
stringData:
  remote: "my-s3"
  remotePath: "projectname"
  configData: |
    [my-s3]
    type = s3
    provider = Minio
    access_key_id = ACCESS_KEY_ID
    secret_access_key = SECRET_ACCESS_KEY
    endpoint = http://minio-release.default:9000
```

Deploy example secret
> `kubectl apply -f example/kubernetes/rclone-secret-example.yaml --namespace kube-system`

3. You can override configuration via PersistentStorage resource definition. Leave volumeAttributes empty if you don't want to. Keys in `volumeAttributes` will be merged with predefined parameters.

```
apiVersion: v1
kind: PersistentVolume
metadata:
  name: data-rclone-example
  labels:
    name: data-rclone-example
spec:
  accessModes:
  - ReadWriteMany
  capacity:
    storage: 10Gi
  storageClassName: rclone
  csi:
    driver: csi-rclone
    volumeHandle: data-id
    volumeAttributes:
      remote: "s3"
      remotePath: "projectname/pvname"
      s3-provider: "Minio"
      s3-endpoint: "http://minio.minio:9000"
      s3-access-key-id: "ACCESS_KEY_ID"
      s3-secret-access-key: "SECRET_ACCESS_KEY"
```

Deploy example definition
> `kubectl apply -f example/kubernetes/nginx-example.yaml`


## PersistentVolumeClaim annotations

- `csi-rclone/umask` - `umask` parameter for `rclone mount`.
- [if configured in storageclass `parameters.pathPattern`] `csi-rclone/storage-path` - Secret name that contains rclone configuration.

Provisioning of other parameters is currently unsupported, create PersistentVolume resource with `volumeAttributes` to define them.

## Building plugin and creating image
Current code is referencing projects repository on github.com. If you fork the repository, you have to change go includes in several places (use search and replace).


1. First push the changed code to remote. The build will use paths from `pkg/` directory.

2. Build the plugin via multistage build and create docker image. `VERSION` file will be used as image tag.
```
make build
```

3. Change docker.io account in `Makefile` and use `make push` to push the image to remote. 
``` 
make push
```
## Changelog

See [CHANGELOG.txt](CHANGELOG.txt)
