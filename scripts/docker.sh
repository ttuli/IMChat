#!/bin/bash
#
# IM2 Docker 镜像构建与推送脚本（供 CI/CD 调用）
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
#                 - registry: 缓存推送到 ${REGISTRY}/<service>:buildcache
#

set -euo pipefail

# ========== 路径 ==========

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

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

declare -A SERVICES=(
    ["idgen-rpc"]="cmd/Idgen/rpc/Dockerfile"
    ["user-rpc"]="cmd/User/rpc/Dockerfile"
    ["auth-rpc"]="cmd/Auth/rpc/Dockerfile"
    ["group-rpc"]="cmd/Group/rpc/Dockerfile"
    ["message-rpc"]="cmd/Message/rpc/Dockerfile"
    ["user-api"]="cmd/User/api/Dockerfile"
    ["auth-api"]="cmd/Auth/api/Dockerfile"
    ["group-api"]="cmd/Group/api/Dockerfile"
    ["message-api"]="cmd/Message/api/Dockerfile"
    ["file-api"]="cmd/File/api/Dockerfile"
    ["websocket"]="cmd/websocket/Dockerfile"
    ["llm-rpc"]="cmd/Llm/rpc/Dockerfile"
    ["llm-api"]="cmd/Llm/api/Dockerfile"
)

# ========== 构建缓存 ==========

# 按服务生成 buildx 缓存参数，结果写入全局数组 CACHE_ARGS
build_cache_args() {
    local service="$1"
    CACHE_ARGS=()
    case "$BUILD_CACHE" in
        none) ;;
        gha)
            # 每个服务独立 scope，避免互相覆盖缓存
            CACHE_ARGS=(
                --cache-from "type=gha,scope=${service}"
                --cache-to   "type=gha,mode=max,scope=${service}"
            )
            ;;
        registry)
            # image-manifest/oci-mediatypes 提升对各类 registry(含 ACR) 的兼容性
            local ref="${REGISTRY}/${service}:buildcache"
            CACHE_ARGS=(
                --cache-from "type=registry,ref=${ref}"
                --cache-to   "type=registry,ref=${ref},mode=max,image-manifest=true,oci-mediatypes=true"
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
    local services_to_build=()

    if [[ ${#requested[@]} -eq 0 || "${requested[0]}" == "all" ]]; then
        log_header "构建并推送所有 IM2 镜像 → ${REGISTRY} (TAG: ${TAG}, CACHE: ${BUILD_CACHE})"
        services_to_build=("${!SERVICES[@]}")
    else
        local svc
        for svc in "${requested[@]}"; do
            if [[ -z "${SERVICES[$svc]+_}" ]]; then
                log_error "未知服务: $svc"
                exit 1
            fi
        done
        log_header "构建并推送 IM2 镜像 [${requested[*]}] → ${REGISTRY} (TAG: ${TAG}, CACHE: ${BUILD_CACHE})"
        services_to_build=("${requested[@]}")
    fi

    local success=0
    local failed_services=()

    local service
    for service in "${services_to_build[@]}"; do
        local dockerfile="${SERVICES[$service]}"
        local image="${REGISTRY}/${service}:${TAG}"
        local latest_image="${REGISTRY}/${service}:latest"

        build_cache_args "$service"

        # buildx --push：构建与推送合并为一步（容器驱动下镜像不落本地，必须直接推送）
        # --provenance=false：保持与旧 docker build 一致的单一 manifest，避免生成 image index
        log_info "构建并推送 ${BOLD}${service}${NC} → ${image}"
        if docker buildx build \
            -f "${PROJECT_ROOT}/${dockerfile}" \
            -t "${image}" \
            -t "${latest_image}" \
            ${CACHE_ARGS[@]+"${CACHE_ARGS[@]}"} \
            --provenance=false \
            --push \
            "${PROJECT_ROOT}"; then
            log_info "${GREEN}✓${NC} ${BOLD}${service}${NC} 构建并推送成功 (${TAG} + latest)"
            ((success++)) || true
        else
            log_error "✗ ${BOLD}${service}${NC} 构建/推送失败"
            failed_services+=("${service}")
        fi
    done

    echo ""
    log_header "构建推送汇总"
    log_info "成功: ${BOLD}${success}${NC}"
    if [[ ${#failed_services[@]} -gt 0 ]]; then
        log_error "失败 (${#failed_services[@]}): ${failed_services[*]}"
        exit 1
    fi

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