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

# ========== 路径 ==========

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"
SERVICES_FILE="${PROJECT_ROOT}/docker-compose.services.yml"

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

# ========== 加载 .env 文件 ==========

ENV_FILE="${PROJECT_ROOT}/.env"
if [[ -f "$ENV_FILE" ]]; then
    while IFS='=' read -r key value; do
        # 跳过注释和空行
        [[ $key =~ ^[[:space:]]*# ]] && continue
        [[ -z $key ]] && continue
        # 移除前后的空格
        key=$(echo "$key" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        value=$(echo "$value" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        export "$key=$value"
    done < "$ENV_FILE"
    log_info "已加载 ${ENV_FILE}"
else
    log_warn "未找到 .env 文件，使用默认配置"
fi

# ========== 配置（按需修改） ==========

# 镜像仓库地址（例如: registry.cn-shenzhen.aliyuncs.com/yournamespace）
REGISTRY="${REGISTRY:-registry.cn-shenzhen.aliyuncs.com/im2}"

# 镜像 Tag（默认固定为 latest，也可以传入如 v1.0.0）
TAG="${TAG:-latest}"

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
    ["llm-rpc"]="cmd/Llm/rpc/Dockerfile"
    ["llm-api"]="cmd/Llm/api/Dockerfile"
    # ["llm-python"]="cmd/Llm/python/Dockerfile"
)

# ========== 核心命令 ==========

# 构建所有镜像并推送到仓库
do_build_push() {
    if [[ "$SERVICE" == "all" ]]; then
        log_header "构建并推送所有 IM2 镜像 → ${REGISTRY} (TAG: ${TAG})"
        local services_to_build=("${!SERVICES[@]}")
    else
        if [[ -z "${SERVICES[$SERVICE]}" ]]; then
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
        local image="${REGISTRY}:${service}-${TAG}"

        local image_latest="${REGISTRY}:${service}-latest"

        log_info "构建 ${BOLD}${service}${NC} → ${image}"
        if docker build \
            -f "${PROJECT_ROOT}/${dockerfile}" \
            -t "${image}" \
            "${PROJECT_ROOT}"; then
            # 同时打上 latest 标签
            docker tag "${image}" "${image_latest}"
            log_info "推送 ${BOLD}${service}${NC} (${TAG} + latest) ..."
            if docker push "${image}" && docker push "${image_latest}"; then
                log_info "${GREEN}✓${NC} ${BOLD}${service}${NC} 推送成功 (${TAG} + latest)"
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

    echo ""
    log_header "清理悬挂镜像"
    log_info "正在清理构建产生的 <none> 镜像以释放空间..."
    docker image prune -f --filter "dangling=true" || true

    echo ""
    log_info "全部完成！TAG=${BOLD}${TAG}${NC}"
}

# 从仓库拉取所有镜像，然后一键启动
do_pull_start() {
    if [[ "$SERVICE" == "all" ]]; then
        log_header "拉取所有 IM2 镜像并启动 (TAG: ${TAG})"
        local services_to_pull=("${!SERVICES[@]}")
        local up_services=""
    else
        if [[ -z "${SERVICES[$SERVICE]}" ]]; then
            log_error "未知服务: $SERVICE"
            exit 1
        fi
        log_header "拉取 IM2 镜像 ${SERVICE} 并启动 (TAG: ${TAG})"
        local services_to_pull=("$SERVICE")
        local up_services="$SERVICE"
    fi

    # 1. 拉取最新镜像
    log_info "从仓库拉取最新镜像..."
    for service in "${services_to_pull[@]}"; do
        local image="${REGISTRY}:${service}-${TAG}"
        log_info "拉取 ${BOLD}${service}${NC} ← ${image}"
        docker pull "${image}" || log_warn "${service} 拉取失败，将使用本地缓存镜像继续"
    done

    # 2. 生成一个临时的 docker-compose.override.yml 来替换 build 为 image 引用
    log_info "生成镜像引用配置 (override)..."
    local override_file="${PROJECT_ROOT}/docker-compose.override.yml"
    {
        echo "version: '3.8'"
        echo "services:"
        for service in "${services_to_pull[@]}"; do
            local image="${REGISTRY}:${service}-${TAG}"
            echo "  ${service}:"
            echo "    image: ${image}"
            echo "    build: !reset null"
        done
    } > "${override_file}"

    # 3. 强制删除可能由其他 compose 项目遗留的同名旧容器
    log_info "清理旧容器（如有）..."
    for service in "${services_to_pull[@]}"; do
        # docker-compose 默认将 project_service 中的连字符转为下划线作为容器名
        local container_name
        container_name="$(basename "${PROJECT_ROOT}" | tr '[:upper:]' '[:lower:]')_${service//-/_}"
        if docker ps -a --format '{{.Names}}' | grep -qx "${container_name}"; then
            log_warn "发现旧容器 ${container_name}，正在删除..."
            docker rm -f "${container_name}" || true
        fi
    done

    # 4. 启动所有服务
    log_info "启动服务..."
    cd "${PROJECT_ROOT}"
    if [[ -z "$up_services" ]]; then
        docker-compose -f "${COMPOSE_FILE}" -f "${SERVICES_FILE}" -f "${override_file}" up -d --remove-orphans
    else
        docker-compose -f "${COMPOSE_FILE}" -f "${SERVICES_FILE}" -f "${override_file}" up -d --remove-orphans $up_services
    fi

    # 清理 override 文件
    rm -f "${override_file}"

    echo ""
    log_header "IM2 启动成功"
    docker-compose -f "${COMPOSE_FILE}" -f "${SERVICES_FILE}" ps
}

# 仅启动基础设施中间件（MySQL/Redis/MongoDB/NATS/Etcd/Nacos）
do_infra() {
    log_header "启动基础设施中间件"
    cd "${PROJECT_ROOT}"
    docker-compose -f "${COMPOSE_FILE}" -f "${SERVICES_FILE}" up -d mysql redis mongodb etcd nats nacos
    log_info "基础设施已启动，可以使用 ${BOLD}./scripts/docker.sh status${NC} 查看状态"
}

# 停止并移除所有容器
do_down() {
    log_header "停止并移除所有 IM2 容器"
    cd "${PROJECT_ROOT}"
    docker-compose -f "${COMPOSE_FILE}" -f "${SERVICES_FILE}" down
    log_info "所有容器已停止"
}

# 查看所有服务状态
do_status() {
    log_header "IM2 容器状态"
    cd "${PROJECT_ROOT}"
    docker-compose -f "${COMPOSE_FILE}" -f "${SERVICES_FILE}" ps
}

do_help() {
    echo ""
    echo "IM2 Docker 镜像管理脚本"
    echo ""
    echo "用法:"
    echo "  \$0 <命令> [服务]"
    echo ""
    echo "命令:"
    echo "  build-push   构建指定微服务镜像并推送到镜像仓库 (默认全部)"
    echo "  pull-start   从镜像仓库拉取指定镜像并启动服务 (默认全部)"
    echo "  infra        仅启动基础设施 (MySQL, Redis, MongoDB, NATS, Etcd, Nacos)"
    echo "  down         停止并移除所有容器 (不删除 Volume 数据)"
    echo "  status       查看所有容器运行状态"
    echo "  help         显示此帮助信息"
    echo ""
    echo "服务:"
    echo "  可选参数，指定要操作的服务名称，或 'all' 表示全部 (默认)。"
    echo "  可用服务:"
    for service in "${!SERVICES[@]}"; do
        echo "    - $service"
    done
    echo ""
    echo "环境变量:"
    echo "  REGISTRY    镜像仓库地址 (默认: registry.cn-shenzhen.aliyuncs.com/im2)"
    echo "  TAG         镜像 Tag (默认: 当前 Git commit SHA)"
    echo ""
    echo "示例:"
    echo "  # 使用默认仓库，构建并推送所有镜像 (Tag 为当前 Git SHA)"
    echo "  \$0 build-push"
    echo ""
    echo "  # 构建并推送 user-api 服务"
    echo "  \$0 build-push user-api"
    echo ""
    echo "  # 构建并推送 llm 相关服务"
    echo "  \$0 build-push llm-rpc"
    echo "  \$0 build-push llm-python"
    echo ""
    echo "  # 指定 Tag 为 v1.0.0 并推送到自定义仓库"
    echo "  REGISTRY=registry.example.com/myns TAG=v1.0.0 \$0 build-push"
    echo ""
    echo "  # 从仓库拉取所有镜像并启动"
    echo "  \$0 pull-start"
    echo ""
    echo "  # 拉取并启动 user-api 服务"
    echo "  \$0 pull-start user-api"
    echo ""
    echo "  # 本地开发：只启动中间件，手动跑微服务代码"
    echo "  \$0 infra"
}

# ========== 参数解析 ==========

ACTION="${1:-help}"
SERVICE="${2:-all}"

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