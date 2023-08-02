
service.go: 相当于service层，用于对外暴露服务，在containerd中一般使用grpc对外暴露服务
store.go: 相当于biz层，是这正的业务实现逻辑，service.go中对外暴露的服务就是依赖biz层