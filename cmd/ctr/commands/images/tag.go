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

package images

import (
	"fmt"

	"github.com/urfave/cli"

	"github.com/containerd/containerd/cmd/ctr/commands"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/pkg/transfer/image"
)

// 对于ctr image tag命令，也就是我们平时所谓的打tag，其实现的原理其实就是使用原始的镜像数据，加上新的tag，这个tag作为新镜像的名字，创建一个
// 新的镜像。实际上镜像在containerd仅仅是是一些元数据，并没有镜像层这些信息，镜像层数据是通过content-service保存的。
var tagCommand = cli.Command{
	Name:        "tag",
	Usage:       "Tag an image",
	ArgsUsage:   "[flags] <source_ref> <target_ref> [<target_ref>, ...]",
	Description: `Tag an image for use in containerd.`,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "force",
			Usage: "Force target_ref to be created, regardless if it already exists",
		},
		cli.BoolTFlag{
			Name:  "local",
			Usage: "Run tag locally rather than through transfer API",
		},
	},
	Action: func(context *cli.Context) error {
		var (
			// 源镜像名
			ref = context.Args().First()
		)
		if ref == "" {
			return fmt.Errorf("please provide an image reference to tag from")
		}
		if context.NArg() <= 1 {
			return fmt.Errorf("please provide an image reference to tag to")
		}

		client, ctx, cancel, err := commands.NewClient(context)
		if err != nil {
			return err
		}
		defer cancel()

		// TODO 如何理解这里的逻辑？
		if !context.BoolT("local") {
			for _, targetRef := range context.Args()[1:] {
				err = client.Transfer(ctx, image.NewStore(ref), image.NewStore(targetRef))
				if err != nil {
					return err
				}
				fmt.Println(targetRef)
			}
			return nil
		}

		// TODO 为什么需要一个Lease资源？
		ctx, done, err := client.WithLease(ctx)
		if err != nil {
			return err
		}
		defer done(ctx)

		imageService := client.ImageService()
		// 获取档期啊需要打Tag的镜像
		image, err := imageService.Get(ctx, ref)
		if err != nil {
			return err
		}
		// Support multiple references for one command run
		// context.Args()[1:]为需要打的目标tag
		for _, targetRef := range context.Args()[1:] {
			image.Name = targetRef
			// Attempt to create the image first
			// 尝试创建镜像，containerd的镜像其实就是元数据，镜像层的信息并不在镜像当中
			// 从这里可以看出，打tag其实就是新建一个镜像，新建立的镜像和原始镜像没有啥关系，和Linux的软连接还不一样
			if _, err = imageService.Create(ctx, image); err != nil {
				// If user has specified force and the image already exists then
				// delete the original image and attempt to create the new one
				// 如果当前目标tag已经存在，并且制定了强制覆盖参数，那么先删除原始镜像，然后创建新的镜像
				if errdefs.IsAlreadyExists(err) && context.Bool("force") {
					if err = imageService.Delete(ctx, targetRef); err != nil {
						return err
					}
					if _, err = imageService.Create(ctx, image); err != nil {
						return err
					}
				} else {
					return err
				}
			}
			fmt.Println(targetRef)
		}
		return nil
	},
}
