#!/bin/bash
#
# IM2 Docker 镜像构建与推送脚本（供 CI/CD 调用）
#
# 构建方式:
#   所有服务共用根目录 Dockerfile 的同一个 builder 阶段（依赖图与全部二进制只编译一次），
#   由 docker buildx bake (docker-bake.hcl) 在一次构建中并行产出各服务镜像。
#
# 用法:
#   构建并推送所有镜像:    TAG=xxx REGISTRY=xxx ./scripts/docker.sh build-push
#   构建并推送指定服务:    TAG=xxx REGISTRY=xxx ./scripts/docker.sh build-push user-api group-rpc
#
# 环境变量:
#   REGISTRY      镜像仓库前缀 (默认 registry.cn-shenzhen.aliyuncs.com/im2)
#   TAG           镜像标签 (默认 latest)
#   BUILD_CACHE   buildx 跨次构建缓存后端: none(默认) | gha | registry
#                 - none:     不使用跨次缓存 (本地构建)
#                 - gha:      GitHub Actions 缓存 (需在 CI 导出 ACTIONS_* 运行时令牌)
#                 - registry: 缓存推送到 ${REGISTRY}/buildcache (需 registry 允许自动创建仓库)
#

set -euo pipefail

# ========== 路径 ==========

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BAKE_FILE="${PROJECT_ROOT}/docker-bake.hcl"

# ========== 颜色 ==========

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log_info()   { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()   { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error()  { echo -e "${RED}[ERROR]${NC} $*"; }
log_header() { echo -e "\n${BOLD}${CYAN}=== $* ===${NC}\n"; }

# ========== 配置 ==========

REGISTRY="${REGISTRY:-registry.cn-shenzhen.aliyuncs.com/im2}"
TAG="${TAG:-latest}"
BUILD_CACHE="${BUILD_CACHE:-none}"

# docker-bake.hcl 通过环境变量读取 REGISTRY/TAG
export REGISTRY TAG

# 可构建的服务列表（与 docker-bake.hcl 中的 target 一一对应）
SERVICES=(
    idgen-rpc user-rpc auth-rpc group-rpc message-rpc llm-rpc
    user-api auth-api group-api message-api file-api llm-api
    websocket
)

# ========== 构建缓存 ==========

# 生成 buildx 缓存参数，结果写入全局数组 CACHE_ARGS
# 所有目标共享同一个 builder 阶段，因此使用统一缓存 scope，公共层只存一份
build_cache_args() {
    CACHE_ARGS=()
    case "$BUILD_CACHE" in
        none) ;;
        gha)
            CACHE_ARGS=(
                --set "*.cache-from=type=gha,scope=im2-shared"
                --set "*.cache-to=type=gha,mode=max,scope=im2-shared"
            )
            ;;
        registry)
            # image-manifest/oci-mediatypes 提升对各类 registry(含 ACR) 的兼容性
            local ref="${REGISTRY}/buildcache:shared"
            CACHE_ARGS=(
                --set "*.cache-from=type=registry,ref=${ref}"
                --set "*.cache-to=type=registry,ref=${ref},mode=max,image-manifest=true,oci-mediatypes=true"
            )
            ;;
        *)
            log_error "未知 BUILD_CACHE: ${BUILD_CACHE} (可选 none|gha|registry)"
            exit 1
            ;;
    esac
}

# ========== 构建并推送 ==========

do_build_push() {
    local requested=("$@")
    local targets=()

    if [[ ${#requested[@]} -eq 0 || "${requested[0]}" == "all" ]]; then
        log_header "构建并推送所有 IM2 镜像 → ${REGISTRY} (TAG: ${TAG}, CACHE: ${BUILD_CACHE})"
        targets=("default")
    else
        local svc known matched
        for svc in "${requested[@]}"; do
            matched=""
            for known in "${SERVICES[@]}"; do
                [[ "$svc" == "$known" ]] && matched=1 && break
            done
            if [[ -z "$matched" ]]; then
                log_error "未知服务: $svc (可选: ${SERVICES[*]})"
                exit 1
            fi
        done
        log_header "构建并推送 IM2 镜像 [${requested[*]}] → ${REGISTRY} (TAG: ${TAG}, CACHE: ${BUILD_CACHE})"
        targets=("${requested[@]}")
    fi

    build_cache_args

    # bake 在同一次构建中并行处理全部目标，builder 阶段跨目标自动去重；
    # 容器驱动下镜像不落本地，构建与推送合并为一步 (--push)
    if ! docker buildx bake \
        -f "$BAKE_FILE" \
        --push \
        ${CACHE_ARGS[@]+"${CACHE_ARGS[@]}"} \
        "${targets[@]}"; then
        log_error "✗ 构建/推送失败: ${targets[*]}"
        exit 1
    fi

    log_info "${GREEN}✓${NC} 构建并推送成功 (${TAG} + latest): ${BOLD}${targets[*]}${NC}"

    log_header "清理悬挂镜像"
    docker image prune -f --filter "dangling=true" || true
    log_info "全部完成！TAG=${BOLD}${TAG}${NC}"
}

# ========== 参数解析 ==========

ACTION="${1:-}"
shift || true

case "$ACTION" in
    build-push) do_build_push "$@" ;;
    *)
        log_error "用法: $0 build-push [服务名... | all]"
        exit 1
        ;;
esac
