#!/bin/bash
#
# IM2 Docker 镜像构建与推送脚本（供 CI/CD 调用）
#
# 用法:
#   构建并推送所有镜像:    TAG=xxx REGISTRY=xxx ./scripts/docker.sh build-push
#   构建并推送单个服务:    TAG=xxx REGISTRY=xxx ./scripts/docker.sh build-push user-api
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

# ========== 构建并推送 ==========

do_build_push() {
    if [[ "$SERVICE" == "all" ]]; then
        log_header "构建并推送所有 IM2 镜像 → ${REGISTRY} (TAG: ${TAG})"
        local services_to_build=("${!SERVICES[@]}")
    else
        if [[ -z "${SERVICES[$SERVICE]+_}" ]]; then
            log_error "未知服务: $SERVICE"
            exit 1
        fi
        log_header "构建并推送 IM2 镜像 ${SERVICE} → ${REGISTRY} (TAG: ${TAG})"
        local services_to_build=("$SERVICE")
    fi

    local success=0
    local failed_services=()

    for service in "${services_to_build[@]}"; do
        local dockerfile="${SERVICES[$service]}"
        local image="${REGISTRY}/${service}:${TAG}"
        local latest_image="${REGISTRY}/${service}:latest"

        log_info "构建 ${BOLD}${service}${NC} → ${image}"
        if docker build \
            -f "${PROJECT_ROOT}/${dockerfile}" \
            -t "${image}" \
            -t "${latest_image}" \
            "${PROJECT_ROOT}"; then

            log_info "推送 ${BOLD}${service}${NC} ..."

            # 一次推送所有标签（registry/name 部分需相同）
            if docker push --all-tags "${REGISTRY}/${service}"; then
                log_info "${GREEN}✓${NC} ${BOLD}${service}${NC} 推送成功 (${TAG} + latest)"
                ((success++)) || true
            else
                log_error "✗ ${BOLD}${service}${NC} 推送失败"
                failed_services+=("${service}")
            fi
        else
            log_error "✗ ${BOLD}${service}${NC} 构建失败"
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
SERVICE="${2:-all}"

case "$ACTION" in
    build-push) do_build_push ;;
    *)
        log_error "用法: $0 build-push [服务名|all]"
        exit 1
        ;;
esac