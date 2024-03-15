//go:build !windows

/*
   Copyright The containerd Authors.

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

package run

import (
	gocontext "context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cmd/ctr/commands"
	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/contrib/apparmor"
	"github.com/containerd/containerd/contrib/nvidia"
	"github.com/containerd/containerd/contrib/seccomp"
	"github.com/containerd/containerd/oci"
	runtimeoptions "github.com/containerd/containerd/pkg/runtimeoptions/v1"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/runtime/v2/runc/options"
	"github.com/containerd/containerd/snapshots"
	"github.com/intel/goresctrl/pkg/blockio"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/urfave/cli"
)

var platformRunFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "runc-binary",
		Usage: "Specify runc-compatible binary",
	},
	cli.StringFlag{
		Name:  "runc-root",
		Usage: "Specify runc-compatible root",
	},
	cli.BoolFlag{
		Name:  "runc-systemd-cgroup",
		Usage: "Start runc with systemd cgroup manager",
	},
	cli.StringFlag{
		Name:  "uidmap",
		Usage: "Run inside a user namespace with the specified UID mapping range; specified with the format `container-uid:host-uid:length`",
	},
	cli.StringFlag{
		Name:  "gidmap",
		Usage: "Run inside a user namespace with the specified GID mapping range; specified with the format `container-gid:host-gid:length`",
	},
	cli.BoolFlag{
		Name:  "remap-labels",
		Usage: "Provide the user namespace ID remapping to the snapshotter via label options; requires snapshotter support",
	},
	cli.BoolFlag{
		Name:  "privileged-without-host-devices",
		Usage: "Don't pass all host devices to privileged container",
	},
	cli.Float64Flag{
		Name:  "cpus",
		Usage: "Set the CFS cpu quota",
		Value: 0.0,
	},
	cli.IntFlag{
		Name:  "cpu-shares",
		Usage: "Set the cpu shares",
		Value: 1024,
	},
}

// NewContainer creates a new container
func NewContainer(
	ctx gocontext.Context,
	client *containerd.Client,
	context *cli.Context, // 命令行框架uvfave的上下文，其中包含了用户命令、参数相关的信息
) (containerd.Container, error) {
	var (
		// 容器ID，由用户创建容器的时候设置，譬如ctr c create docker.io/library/nginx:latest mynginx; 譬如其中的mynginx
		// 就是用户创建容器时设置的容器ID
		id string
		// 用于设置容器运行时的spec，这个spec需要符合OCI RuntimeSpec规范, 譬如可以像如下这样设置
		/*
			{
			    "ociVersion": "0.2.0",
			    "id": "oci-container1",
			    "status": "running",
			    "pid": 4422,
			    "bundle": "/containers/redis",
			    "annotations": {
			        "myKey": "myValue"
			    }
			}

		*/
		config = context.IsSet("config")
	)
	if config { // 通过不同的参数的设置，从不同的位置获取容器ID
		id = context.Args().First() // 获取用户设置的容器ID
	} else {
		id = context.Args().Get(1) // 获取用户设置的容器ID
	}

	var (
		opts  []oci.SpecOpts                // OCI Spec的各种参数设置
		cOpts []containerd.NewContainerOpts // TODO 为什么这里需要使用两个来保存
		spec  containerd.NewContainerOpts
	)

	if config {
		// 获取标签 譬如使用ctr c create --label k1=v1,k2=v2  docker.io/library/nginx:latest my-labeld-nginx命令创建容器，
		// 那么这个容器的变迁就被我设置为了k1=v1,k2=v2
		// TODO k8s的注解、标签是怎么实现的？
		/*
		  通过查看boltdb元数据，我们发现boltdb数据库中存放了创建容器的标签
		  - v1                                                                                       | Path: v1 → default → containers → my-labeld-nginx → labels
		    - default                                                                                | Buckets: 0
		      - containers                                                                           | Pairs: 3
		        - my-labeld-nginx                                                                    |
		          - labels                                                                           |
		            io.containerd.image.config.stop-signal: SIGQUIT                                  |
		            k1: v1,k2=v2                                                                     |
		            maintainer: NGINX Docker Maintainers <docker-maint@nginx.com>                    |
		          + runtime                                                                          |
		          createdat: 010000000edd7cb2d501635fb2ffff                                          |
		          image: docker.io/library/nginx:latest                                              |
		          sandboxid:                                                                         |
		          snapshotKey: my-labeld-nginx                                                       |
		          snapshotter: overlayfs                                                             |
		          spec: 0a3674797065732e636f6e7461696e6572642e696f2f6f70656e636f6e7461696e6572732f727|
		          updatedat: 010000000edd7cb2d501635fb2ffff                                          |
		        + mynginx                                                                            |
		      + content                                                                              |
		      + images                                                                               |
		      + leases                                                                               |
		      + snapshots                                                                            |
		    version: 06
		*/
		cOpts = append(cOpts, containerd.WithContainerLabels(commands.LabelArgs(context.StringSlice("label"))))
		// 获取用户指定的运行时spec
		opts = append(opts, oci.WithSpecFromFile(context.String("config")))
	} else {
		var (
			ref = context.Args().First() // 获取使用的镜像名
			// for container's id is Args[1]
			args = context.Args()[2:] // 获取剩余的参数
		)
		// 1、获取OCI容器默认的运行时配置
		// 2、设置默认的Devices  TODO 这玩意是啥？
		opts = append(opts, oci.WithDefaultSpec(), oci.WithDefaultUnixDevices)
		// 用于在指定的配置文件env-file中设置环境变量
		if ef := context.String("env-file"); ef != "" {
			opts = append(opts, oci.WithEnvFile(ef))
		}
		// 从这里可以看出来 --env-file以及--env参数是可以同时使用的，只不过--env设置的环境变量的优先级更改，会覆盖--env-file配置文件
		// 中的环境变量
		opts = append(opts, oci.WithEnv(context.StringSlice("env")))
		// TODO 设置mount挂载参数
		opts = append(opts, withMounts(context))

		// 是否指定了根文件系统，这里通过设置指定根文件系统同事是containerd snapshotter没有管理的根文件系统
		if context.Bool("rootfs") {
			rootfs, err := filepath.Abs(ref)
			if err != nil {
				return nil, err
			}
			opts = append(opts, oci.WithRootFSPath(rootfs))
			cOpts = append(cOpts, containerd.WithContainerLabels(commands.LabelArgs(context.StringSlice("label"))))
		} else {
			// 获取用户指定的快照插件
			snapshotter := context.String("snapshotter")
			var image containerd.Image
			// 根据镜像名查询  TODO 如果此时镜像不存在是否会自动下载？
			i, err := client.ImageService().Get(ctx, ref)
			if err != nil {
				return nil, err
			}
			// TODO 设置平台，containerd中的平台有啥用？
			if ps := context.String("platform"); ps != "" {
				platform, err := platforms.Parse(ps)
				if err != nil {
					return nil, err
				}
				// 用于设置镜像的平台信息
				image = containerd.NewImageWithPlatform(client, i, platforms.Only(platform))
			} else {
				image = containerd.NewImage(client, i)
			}

			// TODO 什么叫做镜像的UnPack，意思是镜像还没有解压缩？
			unpacked, err := image.IsUnpacked(ctx, snapshotter)
			if err != nil {
				return nil, err
			}
			if !unpacked {
				if err := image.Unpack(ctx, snapshotter); err != nil {
					return nil, err
				}
			}
			labels := buildLabels(commands.LabelArgs(context.StringSlice("label")), image.Labels())
			opts = append(opts, oci.WithImageConfig(image))
			cOpts = append(cOpts,
				containerd.WithImage(image),
				containerd.WithImageConfigLabels(image),
				containerd.WithAdditionalContainerLabels(labels),
				containerd.WithSnapshotter(snapshotter))
			// TODO 什么是uid, gid是一个Map?
			if uidmap, gidmap := context.String("uidmap"), context.String("gidmap"); uidmap != "" && gidmap != "" {
				uidMap, err := parseIDMapping(uidmap)
				if err != nil {
					return nil, err
				}
				gidMap, err := parseIDMapping(gidmap)
				if err != nil {
					return nil, err
				}
				opts = append(opts,
					oci.WithUserNamespace([]specs.LinuxIDMapping{uidMap}, []specs.LinuxIDMapping{gidMap}))
				// use snapshotter opts or the remapped snapshot support to shift the filesystem
				// currently the only snapshotter known to support the labels is fuse-overlayfs:
				// https://github.com/AkihiroSuda/containerd-fuse-overlayfs
				if context.Bool("remap-labels") {
					cOpts = append(cOpts, containerd.WithNewSnapshot(id, image,
						containerd.WithRemapperLabels(0, uidMap.HostID, 0, gidMap.HostID, uidMap.Size)))
				} else {
					cOpts = append(cOpts, containerd.WithRemappedSnapshot(id, image, uidMap.HostID, gidMap.HostID))
				}
			} else {
				// Even when "read-only" is set, we don't use KindView snapshot here. (#1495)
				// We pass writable snapshot to the OCI runtime, and the runtime remounts it as read-only,
				// after creating some mount points on demand.
				// For some snapshotter, such as overlaybd, it can provide 2 kind of writable snapshot(overlayfs dir or block-device)
				// by command label values.
				// TODO 快照标签又是拿来干嘛的？
				cOpts = append(cOpts, containerd.WithNewSnapshot(id, image,
					snapshots.WithLabels(commands.LabelArgs(context.StringSlice("snapshotter-label")))))
			}
			cOpts = append(cOpts, containerd.WithImageStopSignal(image, "SIGTERM"))
		}
		// TODO 为什么要设置为根文件系统为只读模式？  什么场景适合设置？
		if context.Bool("read-only") {
			opts = append(opts, oci.WithRootFSReadonly())
		}

		// 镜像启动的参数
		if len(args) > 0 {
			opts = append(opts, oci.WithProcessArgs(args...))
		}

		// TODO 这玩意应该是通过DockerFile中的WorkDir设置的
		if cwd := context.String("cwd"); cwd != "" {
			opts = append(opts, oci.WithProcessCwd(cwd))
		}
		// 设置容器进程中的用户
		if user := context.String("user"); user != "" {
			opts = append(opts, oci.WithUser(user), oci.WithAdditionalGIDs(user))
		}
		// TODO 如何理解Linux的TTY设备？
		if context.Bool("tty") {
			opts = append(opts, oci.WithTTY)
		}

		// 当前容器是否以特权的方式启动
		privileged := context.Bool("privileged")
		// TODO 这玩意是什么参数，似乎在ctr container create命令中没有看到这个参数，这个参数似乎并不是给用户使用的
		privilegedWithoutHostDevices := context.Bool("privileged-without-host-devices")
		if privilegedWithoutHostDevices && !privileged {
			return nil, fmt.Errorf("can't use 'privileged-without-host-devices' without 'privileged' specified")
		}
		if privileged {
			if privilegedWithoutHostDevices {
				opts = append(opts, oci.WithPrivileged)
			} else {
				opts = append(opts, oci.WithPrivileged, oci.WithAllDevicesAllowed, oci.WithHostDevices)
			}
		}

		// TODO 设置容器启动的网络模式
		if context.Bool("net-host") {
			hostname, err := os.Hostname()
			if err != nil {
				return nil, fmt.Errorf("get hostname: %w", err)
			}
			opts = append(opts,
				oci.WithHostNamespace(specs.NetworkNamespace),
				oci.WithHostHostsFile,
				oci.WithHostResolvconf,
				oci.WithEnv([]string{fmt.Sprintf("HOSTNAME=%s", hostname)}),
			)
		}
		// TODO 设置OCI注解,这玩意和label有何区别
		if annoStrings := context.StringSlice("annotation"); len(annoStrings) > 0 {
			annos, err := commands.AnnotationArgs(annoStrings)
			if err != nil {
				return nil, err
			}
			opts = append(opts, oci.WithAnnotations(annos))
		}

		// TODO 似乎也没有看到这个参数的入口 ctr container create命令似乎没有用于指定cni的参数
		if context.Bool("cni") {
			cniMeta := &commands.NetworkMetaData{EnableCni: true}
			cOpts = append(cOpts, containerd.WithContainerExtension(commands.CtrCniMetadataExtension, cniMeta))
		}
		// 设置linux capability
		if caps := context.StringSlice("cap-add"); len(caps) > 0 {
			for _, cap := range caps {
				if !strings.HasPrefix(cap, "CAP_") {
					return nil, fmt.Errorf("capabilities must be specified with 'CAP_' prefix")
				}
			}
			opts = append(opts, oci.WithAddedCapabilities(caps))
		}

		// 去除linux capability
		if caps := context.StringSlice("cap-drop"); len(caps) > 0 {
			for _, cap := range caps {
				if !strings.HasPrefix(cap, "CAP_") {
					return nil, fmt.Errorf("capabilities must be specified with 'CAP_' prefix")
				}
			}
			opts = append(opts, oci.WithDroppedCapabilities(caps))
		}

		// TODO seccomp到底是啥？
		seccompProfile := context.String("seccomp-profile")

		if !context.Bool("seccomp") && seccompProfile != "" {
			return nil, fmt.Errorf("seccomp must be set to true, if using a custom seccomp-profile")
		}

		if context.Bool("seccomp") {
			if seccompProfile != "" {
				opts = append(opts, seccomp.WithProfile(seccompProfile))
			} else {
				opts = append(opts, seccomp.WithDefaultProfile())
			}
		}

		// 和selinux类似的东西，用于限制用户权限
		if s := context.String("apparmor-default-profile"); len(s) > 0 {
			opts = append(opts, apparmor.WithDefaultProfile(s))
		}

		if s := context.String("apparmor-profile"); len(s) > 0 {
			if len(context.String("apparmor-default-profile")) > 0 {
				return nil, fmt.Errorf("apparmor-profile conflicts with apparmor-default-profile")
			}
			opts = append(opts, apparmor.WithProfile(s))
		}

		// TODO 这个参数如果需要使用的话应该怎么设置？
		if cpus := context.Float64("cpus"); cpus > 0.0 {
			var (
				period = uint64(100000)
				quota  = int64(cpus * 100000.0)
			)
			opts = append(opts, oci.WithCPUCFS(quota, period))
		}

		// TODO 这个参数如果需要使用的话应该怎么设置？
		if shares := context.Int("cpu-shares"); shares > 0 {
			opts = append(opts, oci.WithCPUShares(uint64(shares)))
		}

		// TODO 这个参数如果需要使用的话应该怎么设置？
		quota := context.Int64("cpu-quota")
		period := context.Uint64("cpu-period")
		if quota != -1 || period != 0 {
			if cpus := context.Float64("cpus"); cpus > 0.0 {
				return nil, errors.New("cpus and quota/period should be used separately")
			}
			opts = append(opts, oci.WithCPUCFS(quota, period))
		}

		joinNs := context.StringSlice("with-ns")
		for _, ns := range joinNs {
			nsType, nsPath, ok := strings.Cut(ns, ":")
			if !ok {
				return nil, errors.New("joining a Linux namespace using --with-ns requires the format 'nstype:path'")
			}
			if !validNamespace(nsType) {
				return nil, errors.New("the Linux namespace type specified in --with-ns is not valid: " + nsType)
			}
			opts = append(opts, oci.WithLinuxNamespace(specs.LinuxNamespace{
				Type: specs.LinuxNamespaceType(nsType),
				Path: nsPath,
			}))
		}
		if context.IsSet("gpus") {
			opts = append(opts, nvidia.WithGPUs(nvidia.WithDevices(context.IntSlice("gpus")...), nvidia.WithAllCapabilities))
		}
		if context.IsSet("allow-new-privs") {
			opts = append(opts, oci.WithNewPrivileges)
		}
		// TODO 设置cgroup
		if context.IsSet("cgroup") {
			// NOTE: can be set to "" explicitly for disabling cgroup.
			opts = append(opts, oci.WithCgroup(context.String("cgroup")))
		}
		limit := context.Uint64("memory-limit")
		if limit != 0 {
			opts = append(opts, oci.WithMemoryLimit(limit))
		}

		// TODO 这里面指定的device代表啥
		for _, dev := range context.StringSlice("device") {
			opts = append(opts, oci.WithDevices(dev, "", "rwm"))
		}

		// TODO 什么叫做rootfs传播
		rootfsPropagation := context.String("rootfs-propagation")
		if rootfsPropagation != "" {
			opts = append(opts, func(_ gocontext.Context, _ oci.Client, _ *containers.Container, s *oci.Spec) error {
				if s.Linux != nil {
					s.Linux.RootfsPropagation = rootfsPropagation
				} else {
					s.Linux = &specs.Linux{
						RootfsPropagation: rootfsPropagation,
					}
				}

				return nil
			})
		}

		// TODO blackio是啥？
		if c := context.String("blockio-config-file"); c != "" {
			if err := blockio.SetConfigFromFile(c, false); err != nil {
				return nil, fmt.Errorf("blockio-config-file error: %w", err)
			}
		}

		if c := context.String("blockio-class"); c != "" {
			if linuxBlockIO, err := blockio.OciLinuxBlockIO(c); err == nil {
				opts = append(opts, oci.WithBlockIO(linuxBlockIO))
			} else {
				return nil, fmt.Errorf("blockio-class error: %w", err)
			}
		}

		// TODO rdt是啥
		if c := context.String("rdt-class"); c != "" {
			opts = append(opts, oci.WithRdt(c, "", ""))
		}
		if hostname := context.String("hostname"); hostname != "" {
			opts = append(opts, oci.WithHostname(hostname))
		}
	}

	// 获取容器的运行时，一般就是runc
	runtimeOpts, err := getRuntimeOptions(context)
	if err != nil {
		return nil, err
	}
	cOpts = append(cOpts, containerd.WithRuntime(context.String("runtime"), runtimeOpts))

	opts = append(opts, oci.WithAnnotations(commands.LabelArgs(context.StringSlice("label"))))
	var s specs.Spec
	spec = containerd.WithSpec(&s, opts...)

	cOpts = append(cOpts, spec)

	// oci.WithImageConfig (WithUsername, WithUserID) depends on access to rootfs for resolving via
	// the /etc/{passwd,group} files. So cOpts needs to have precedence over opts.
	return client.NewContainer(ctx, id, cOpts...)
}

func getRuncOptions(context *cli.Context) (*options.Options, error) {
	runtimeOpts := &options.Options{}
	if runcBinary := context.String("runc-binary"); runcBinary != "" {
		runtimeOpts.BinaryName = runcBinary
	}
	if context.Bool("runc-systemd-cgroup") {
		if context.String("cgroup") == "" {
			// runc maps "machine.slice:foo:deadbeef" to "/machine.slice/foo-deadbeef.scope"
			return nil, errors.New("option --runc-systemd-cgroup requires --cgroup to be set, e.g. \"machine.slice:foo:deadbeef\"")
		}
		runtimeOpts.SystemdCgroup = true
	}
	if root := context.String("runc-root"); root != "" {
		runtimeOpts.Root = root
	}

	return runtimeOpts, nil
}

func getRuntimeOptions(context *cli.Context) (interface{}, error) {
	// validate first
	if (context.String("runc-binary") != "" || context.Bool("runc-systemd-cgroup")) &&
		context.String("runtime") != "io.containerd.runc.v2" {
		return nil, errors.New("specifying runc-binary and runc-systemd-cgroup is only supported for \"io.containerd.runc.v2\" runtime")
	}

	if context.String("runtime") == "io.containerd.runc.v2" {
		return getRuncOptions(context)
	}

	if configPath := context.String("runtime-config-path"); configPath != "" {
		return &runtimeoptions.Options{
			ConfigPath: configPath,
		}, nil
	}

	return nil, nil
}

func parseIDMapping(mapping string) (specs.LinuxIDMapping, error) {
	// We expect 3 parts, but limit to 4 to allow detection of invalid values.
	parts := strings.SplitN(mapping, ":", 4)
	if len(parts) != 3 {
		return specs.LinuxIDMapping{}, errors.New("user namespace mappings require the format `container-id:host-id:size`")
	}
	cID, err := strconv.ParseUint(parts[0], 0, 32)
	if err != nil {
		return specs.LinuxIDMapping{}, fmt.Errorf("invalid container id for user namespace remapping: %w", err)
	}
	hID, err := strconv.ParseUint(parts[1], 0, 32)
	if err != nil {
		return specs.LinuxIDMapping{}, fmt.Errorf("invalid host id for user namespace remapping: %w", err)
	}
	size, err := strconv.ParseUint(parts[2], 0, 32)
	if err != nil {
		return specs.LinuxIDMapping{}, fmt.Errorf("invalid size for user namespace remapping: %w", err)
	}
	return specs.LinuxIDMapping{
		ContainerID: uint32(cID),
		HostID:      uint32(hID),
		Size:        uint32(size),
	}, nil
}

func validNamespace(ns string) bool {
	linuxNs := specs.LinuxNamespaceType(ns)
	switch linuxNs {
	case specs.PIDNamespace,
		specs.NetworkNamespace,
		specs.UTSNamespace,
		specs.MountNamespace,
		specs.UserNamespace,
		specs.IPCNamespace,
		specs.CgroupNamespace:
		return true
	default:
		return false
	}
}

func getNetNSPath(_ gocontext.Context, task containerd.Task) (string, error) {
	return fmt.Sprintf("/proc/%d/ns/net", task.Pid()), nil
}
