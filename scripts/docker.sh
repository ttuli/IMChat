#!/bin/bash
#
# IM2 Docker 镜像管理脚本
#
# 用法:
#   构建并推送所有镜像到镜像仓库:  ./scripts/docker.sh build-push
#   从镜像仓库拉取并启动所有服务:  ./scripts/docker.sh pull-start
#   仅启动基础设施中间件:          ./scripts/docker.sh infra
#   停止并移除所有容器:            ./scripts/docker.sh down
#   查看所有服务状态:              ./scripts/docker.sh status
#   查看帮助:                      ./scripts/docker.sh help
#

set -euo pipefail

# ========== 配置（按需修改） ==========

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# 镜像仓库地址（例如: registry.cn-shenzhen.aliyuncs.com/yournamespace）
REGISTRY="${REGISTRY:-registry.cn-shenzhen.aliyuncs.com/im2}"

# 镜像 Tag（默认使用 Git commit SHA，也可以传入如 v1.0.0）
TAG="${TAG:-$(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo "latest")}"

# 所有微服务：名称 -> Dockerfile 路径
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
)

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

# ========== 核心命令 ==========

# 构建所有镜像并推送到仓库
do_build_push() {
    log_header "构建并推送所有 IM2 镜像 → ${REGISTRY} (TAG: ${TAG})"

    local success=0
    local failed_services=()

    for service in "${!SERVICES[@]}"; do
        local dockerfile="${SERVICES[$service]}"
        local image="${REGISTRY}/${service}:${TAG}"

        log_info "构建 ${BOLD}${service}${NC} → ${image}"
        if docker build \
            -f "${PROJECT_ROOT}/${dockerfile}" \
            -t "${image}" \
            "${PROJECT_ROOT}"; then
            log_info "推送 ${BOLD}${service}${NC} ..."
            if docker push "${image}"; then
                log_info "${GREEN}✓${NC} ${BOLD}${service}${NC} 推送成功"
                ((success++)) || true
            else
                log_error "✗ ${BOLD}${service}${NC} 推送失败"
                failed_services+=("$service")
            fi
        else
            log_error "✗ ${BOLD}${service}${NC} 构建失败"
            failed_services+=("$service")
        fi
    done

    echo ""
    log_header "构建推送汇总"
    log_info "成功: ${BOLD}${success}${NC}"
    if [[ ${#failed_services[@]} -gt 0 ]]; then
        log_error "失败 (${#failed_services[@]}): ${failed_services[*]}"
        exit 1
    fi

    # 同时打上 latest tag
    if [[ "${TAG}" != "latest" ]]; then
        log_info "同步更新 latest 标签..."
        for service in "${!SERVICES[@]}"; do
            docker tag "${REGISTRY}/${service}:${TAG}" "${REGISTRY}/${service}:latest"
            docker push "${REGISTRY}/${service}:latest"
        done
    fi

    log_info "全部完成！TAG=${BOLD}${TAG}${NC}"
}

# 从仓库拉取所有镜像，然后一键启动
do_pull_start() {
    log_header "拉取所有 IM2 镜像并启动 (TAG: ${TAG})"

    # 1. 更新 docker-compose.yml 中的镜像引用，然后 pull
    log_info "从仓库拉取最新镜像..."
    for service in "${!SERVICES[@]}"; do
        local image="${REGISTRY}/${service}:${TAG}"
        log_info "拉取 ${BOLD}${service}${NC} ← ${image}"
        docker pull "${image}" || log_warn "${service} 拉取失败，将使用本地缓存镜像继续"
    done

    # 2. 生成一个临时的 docker-compose.override.yml 来替换 build 为 image 引用
    log_info "生成镜像引用配置 (override)..."
    local override_file="${PROJECT_ROOT}/docker-compose.override.yml"
    {
        echo "version: '3.8'"
        echo "services:"
        for service in "${!SERVICES[@]}"; do
            local image="${REGISTRY}/${service}:${TAG}"
            echo "  ${service}:"
            echo "    image: ${image}"
            echo "    build: !reset null"
        done
    } > "${override_file}"

    # 3. 启动所有服务
    log_info "启动所有服务..."
    cd "${PROJECT_ROOT}"
    docker-compose -f docker-compose.yml -f "${override_file}" up -d

    # 清理 override 文件
    rm -f "${override_file}"

    echo ""
    log_header "IM2 启动成功"
    docker-compose ps
}

# 仅启动基础设施中间件（MySQL/Redis/MongoDB/NATS/Etcd/Nacos）
do_infra() {
    log_header "启动基础设施中间件"
    cd "${PROJECT_ROOT}"
    docker-compose up -d mysql redis mongodb etcd nats nacos
    log_info "基础设施已启动，可以使用 ${BOLD}./scripts/docker.sh status${NC} 查看状态"
}

# 停止并移除所有容器
do_down() {
    log_header "停止并移除所有 IM2 容器"
    cd "${PROJECT_ROOT}"
    docker-compose down
    log_info "所有容器已停止"
}

# 查看所有服务状态
do_status() {
    log_header "IM2 容器状态"
    cd "${PROJECT_ROOT}"
    docker-compose ps
}

do_help() {
    cat <<EOF

${BOLD}${CYAN}IM2 Docker 镜像管理脚本${NC}

${BOLD}用法:${NC}
  $0 <命令> [选项]

${BOLD}命令:${NC}
  ${BOLD}build-push${NC}   构建所有微服务镜像并推送到镜像仓库
  ${BOLD}pull-start${NC}   从镜像仓库拉取最新镜像并启动所有容器
  ${BOLD}infra${NC}        仅启动基础设施 (MySQL, Redis, MongoDB, NATS, Etcd, Nacos)
  ${BOLD}down${NC}         停止并移除所有容器 (不删除 Volume 数据)
  ${BOLD}status${NC}       查看所有容器运行状态
  ${BOLD}help${NC}         显示此帮助信息

${BOLD}环境变量:${NC}
  ${BOLD}REGISTRY${NC}     镜像仓库地址 (默认: registry.cn-shenzhen.aliyuncs.com/im2)
  ${BOLD}TAG${NC}          镜像 Tag    (默认: 当前 Git commit SHA)

${BOLD}示例:${NC}
  # 使用默认仓库，构建并推送 (Tag 为当前 Git SHA)
  $0 build-push

  # 指定 Tag 为 v1.0.0 并推送到自定义仓库
  REGISTRY=registry.example.com/myns TAG=v1.0.0 $0 build-push

  # 从仓库拉取 v1.0.0 的镜像并启动
  TAG=v1.0.0 $0 pull-start

  # 本地开发：只启动中间件，手动跑微服务代码
  $0 infra

EOF
}

# ========== 参数解析 ==========

ACTION="${1:-help}"

case "$ACTION" in
    build-push) do_build_push ;;
    pull-start) do_pull_start ;;
    infra)      do_infra      ;;
    down)       do_down       ;;
    status)     do_status     ;;
    help|--help|-h) do_help  ;;
    *)
        log_error "未知命令: $ACTION"
        do_help
        exit 1
        ;;
esac
