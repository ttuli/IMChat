// IM2 服务镜像构建定义（docker buildx bake）
//
// 所有服务共用根目录 Dockerfile：builder 阶段一次性编译全部二进制，
// bake 在同一次构建中并行产出各服务镜像，公共阶段自动去重。
//
// 通常经由 scripts/docker.sh 调用，也可直接使用:
//   REGISTRY=xxx TAG=xxx docker buildx bake --push             # 全部服务
//   REGISTRY=xxx TAG=xxx docker buildx bake --push user-api    # 指定服务

variable "REGISTRY" {
  default = "registry.cn-shenzhen.aliyuncs.com/im2"
}

variable "TAG" {
  default = "latest"
}

group "default" {
  targets = ["service"]
}

target "service" {
  matrix = {
    // name 同时是 target 名、镜像名与 builder 阶段产物 /out/<name> 的文件名
    item = [
      { name = "idgen-rpc", etc = "internal/apps/Idgen/rpc/etc" },
      { name = "user-rpc", etc = "internal/apps/User/rpc/etc" },
      { name = "auth-rpc", etc = "internal/apps/Auth/rpc/etc" },
      { name = "group-rpc", etc = "internal/apps/Group/rpc/etc" },
      { name = "message-rpc", etc = "internal/apps/Message/rpc/etc" },
      { name = "llm-rpc", etc = "internal/apps/Llm/rpc/etc" },
      { name = "user-api", etc = "internal/apps/User/api/etc" },
      { name = "auth-api", etc = "internal/apps/Auth/api/etc" },
      { name = "group-api", etc = "internal/apps/Group/api/etc" },
      { name = "message-api", etc = "internal/apps/Message/api/etc" },
      { name = "file-api", etc = "internal/apps/File/api/etc" },
      { name = "llm-api", etc = "internal/apps/Llm/api/etc" },
      { name = "websocket", etc = "internal/apps/websocket/gateway/etc" },
    ]
  }
  name       = item.name
  context    = "."
  dockerfile = "Dockerfile"
  target     = "runtime"
  platforms  = ["linux/amd64"]
  args = {
    SERVICE = item.name
    ETC_DIR = item.etc
  }
  tags = [
    "${REGISTRY}/${item.name}:${TAG}",
    "${REGISTRY}/${item.name}:latest",
  ]
  // 与旧 docker build 保持一致的单一 manifest，不生成 provenance/image index
  attest = ["type=provenance,disabled=true"]
}
