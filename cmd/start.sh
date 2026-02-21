#!/bin/bash
#
# IM2 一键启动/停止/状态管理脚本
#
# 用法:
#   启动所有服务:         ./start.sh
#   排除某些服务:         ./start.sh --exclude idgen-rpc,file-api
#   停止所有服务:         ./start.sh --stop
#   查看服务状态:         ./start.sh --status
#   查看帮助:             ./start.sh --help
#
# 服务名列表:
#   RPC: idgen-rpc, user-rpc, auth-rpc, group-rpc, message-rpc
#   API: user-api, auth-api, group-api, message-api, file-api, websocket
#

set -euo pipefail

# ========== 配置 ==========

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_DIR="/tmp/im2"
PID_FILE="$PID_DIR/pids"
LOG_DIR="/var/log/im"

# RPC 服务按顺序启动
RPC_SERVICES=("idgen-rpc" "user-rpc" "auth-rpc" "group-rpc" "message-rpc")

# API 服务无序启动
API_SERVICES=("user-api" "auth-api" "group-api" "message-api" "file-api" "websocket")

# RPC 启动间隔（秒）
RPC_WAIT=2

# 服务名到 main.go 路径的映射
declare -A SERVICE_ENTRY=(
    ["idgen-rpc"]="cmd/Idgen/rpc/main.go"
    ["user-rpc"]="cmd/User/rpc/main.go"
    ["auth-rpc"]="cmd/Auth/rpc/main.go"
    ["group-rpc"]="cmd/Group/rpc/main.go"
    ["message-rpc"]="cmd/Message/rpc/main.go"
    ["user-api"]="cmd/User/api/main.go"
    ["auth-api"]="cmd/Auth/api/main.go"
    ["group-api"]="cmd/Group/api/main.go"
    ["message-api"]="cmd/Message/api/main.go"
    ["file-api"]="cmd/File/api/main.go"
    ["websocket"]="cmd/websocket/main.go"
)

# ========== 颜色 ==========

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# ========== 工具函数 ==========

log_info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; }
log_header()  { echo -e "\n${BOLD}${CYAN}=== $* ===${NC}\n"; }

ensure_dirs() {
    mkdir -p "$PID_DIR"
    mkdir -p "$LOG_DIR" 2>/dev/null || true
}

# 检查服务名是否有效
validate_service_name() {
    local name="$1"
    if [[ -z "${SERVICE_ENTRY[$name]+x}" ]]; then
        log_error "未知的服务名: ${BOLD}$name${NC}"
        log_error "可用的服务名: ${!SERVICE_ENTRY[*]}"
        return 1
    fi
}

# 检查服务是否在排除列表中
is_excluded() {
    local service="$1"
    for excluded in "${EXCLUDE_LIST[@]}"; do
        if [[ "$service" == "$excluded" ]]; then
            return 0
        fi
    done
    return 1
}

# 记录 PID
save_pid() {
    local service="$1"
    local pid="$2"
    echo "$service $pid" >> "$PID_FILE"
}

# 启动单个服务
start_service() {
    local service="$1"
    local entry="${SERVICE_ENTRY[$service]}"
    local log_file="$LOG_DIR/${service}.log"
    local bin_dir="$PROJECT_ROOT/build"
    local bin_file="$bin_dir/$service"

    log_info "正在编译 ${BOLD}$service${NC} ..."
    mkdir -p "$bin_dir"
    cd "$PROJECT_ROOT"
    if ! go build -o "$bin_file" "./$entry" 2>>"$log_file"; then
        log_error "${BOLD}$service${NC} 编译失败，请检查日志: $log_file"
        return 1
    fi

    log_info "正在启动 ${BOLD}$service${NC} ..."
    nohup "$bin_file" >> "$log_file" 2>&1 &
    local pid=$!

    save_pid "$service" "$pid"
    log_info "${BOLD}$service${NC} 已启动 (PID: ${BOLD}$pid${NC}, 日志: $log_file)"
}

# ========== 主要命令 ==========

do_start() {
    ensure_dirs

    # 如果有旧的 PID 文件且存在运行中的进程，先停止它们
    if [[ -f "$PID_FILE" ]]; then
        local has_running=false
        while IFS=' ' read -r service pid; do
            [[ -z "$service" || -z "$pid" ]] && continue
            if kill -0 "$pid" 2>/dev/null; then
                has_running=true
                break
            fi
        done < "$PID_FILE"

        if [[ "$has_running" == true ]]; then
            log_warn "检测到旧服务仍在运行，正在自动停止..."
            do_stop
        fi
    fi

    # 清除旧的 PID 文件
    > "$PID_FILE"

    local started_count=0
    local skipped_count=0

    # 1. 按顺序启动 RPC 服务
    log_header "启动 RPC 服务"
    for service in "${RPC_SERVICES[@]}"; do
        if is_excluded "$service"; then
            log_warn "跳过 ${BOLD}$service${NC} (已排除)"
            ((skipped_count++)) || true
            continue
        fi
        start_service "$service"
        ((started_count++)) || true
        log_info "等待 ${RPC_WAIT}s 确保 $service 就绪..."
        sleep "$RPC_WAIT"
    done

    # 2. 并发启动所有 API 服务
    log_header "启动 API 服务"
    for service in "${API_SERVICES[@]}"; do
        if is_excluded "$service"; then
            log_warn "跳过 ${BOLD}$service${NC} (已排除)"
            ((skipped_count++)) || true
            continue
        fi
        start_service "$service"
        ((started_count++)) || true
    done

    # 3. 汇总
    echo ""
    log_header "启动完成"
    log_info "已启动: ${BOLD}$started_count${NC} 个服务"
    if [[ $skipped_count -gt 0 ]]; then
        log_warn "已跳过: ${BOLD}$skipped_count${NC} 个服务"
    fi
    log_info "PID 文件: $PID_FILE"
    echo ""
}

do_stop() {
    log_header "停止所有 IM2 服务"

    local stopped=0
    local failed=0

    if [[ -f "$PID_FILE" ]]; then
        while IFS=' ' read -r service pid; do
            [[ -z "$service" || -z "$pid" ]] && continue
            if kill -0 "$pid" 2>/dev/null; then
                log_info "正在停止 ${BOLD}$service${NC} (PID: $pid)..."
                kill "$pid" 2>/dev/null
                ((stopped++)) || true
            else
                log_warn "${BOLD}$service${NC} (PID: $pid) 已不存在"
                ((failed++)) || true
            fi
        done < "$PID_FILE"

        # 等待进程退出
        if [[ $stopped -gt 0 ]]; then
            log_info "等待进程退出..."
            sleep 2

            # 强制终止仍在运行的进程
            while IFS=' ' read -r service pid; do
                [[ -z "$service" || -z "$pid" ]] && continue
                if kill -0 "$pid" 2>/dev/null; then
                    log_warn "${BOLD}$service${NC} (PID: $pid) 未正常退出，强制终止..."
                    kill -9 "$pid" 2>/dev/null || true
                fi
            done < "$PID_FILE"
        fi

        rm -f "$PID_FILE"
    else
        log_warn "PID 文件不存在: $PID_FILE"
    fi

    # 清理编译产生的二进制文件
    local bin_dir="$PROJECT_ROOT/build"
    if [[ -d "$bin_dir" ]]; then
        # 杀掉任何仍在运行的 bin 目录下的残留进程
        for bin_file in "$bin_dir"/*; do
            [[ -f "$bin_file" ]] || continue
            pkill -f "^$bin_file" 2>/dev/null || true
        done
        rm -rf "$bin_dir"
        log_info "已清理编译产物: $bin_dir"
    fi

    echo ""
    log_info "已停止 ${BOLD}$stopped${NC} 个服务"
    if [[ $failed -gt 0 ]]; then
        log_warn "${BOLD}$failed${NC} 个服务已不存在"
    fi
    echo ""
}

do_status() {
    log_header "IM2 服务状态"

    if [[ ! -f "$PID_FILE" ]]; then
        log_warn "PID 文件不存在: $PID_FILE"
        log_warn "服务可能未通过此脚本启动"
        return 0
    fi

    printf "  ${BOLD}%-20s %-10s %-10s${NC}\n" "服务" "PID" "状态"
    printf "  %-20s %-10s %-10s\n" "--------------------" "----------" "----------"

    local running=0
    local dead=0

    while IFS=' ' read -r service pid; do
        [[ -z "$service" || -z "$pid" ]] && continue
        if kill -0 "$pid" 2>/dev/null; then
            printf "  %-20s %-10s ${GREEN}%-10s${NC}\n" "$service" "$pid" "运行中"
            ((running++)) || true
        else
            printf "  %-20s %-10s ${RED}%-10s${NC}\n" "$service" "$pid" "已停止"
            ((dead++)) || true
        fi
    done < "$PID_FILE"

    echo ""
    log_info "运行中: ${BOLD}$running${NC}  已停止: ${BOLD}$dead${NC}"
    echo ""
}

do_help() {
    cat <<EOF

${BOLD}${CYAN}IM2 一键服务管理脚本${NC}

${BOLD}用法:${NC}
  $0 [选项]

${BOLD}选项:${NC}
  --exclude <服务列表>   排除指定服务，多个服务用逗号分隔
  --stop                 停止所有已启动的服务
  --status               查看所有服务运行状态
  --help                 显示此帮助信息

${BOLD}服务名列表:${NC}
  ${BOLD}RPC 服务（按以下顺序启动）:${NC}
    idgen-rpc, user-rpc, auth-rpc, group-rpc, message-rpc

  ${BOLD}API 服务（无序启动）:${NC}
    user-api, auth-api, group-api, message-api, file-api, websocket

${BOLD}示例:${NC}
  # 启动所有服务
  $0

  # 排除 file-api 和 websocket
  $0 --exclude file-api,websocket

  # 只排除 idgen-rpc
  $0 --exclude idgen-rpc

  # 停止所有服务
  $0 --stop

  # 查看服务状态
  $0 --status

EOF
}

# ========== 参数解析 ==========

ACTION="start"
EXCLUDE_LIST=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --exclude)
            if [[ -z "${2:-}" ]]; then
                log_error "--exclude 需要指定服务列表"
                exit 1
            fi
            IFS=',' read -ra EXCLUDE_LIST <<< "$2"
            # 验证所有排除的服务名
            for svc in "${EXCLUDE_LIST[@]}"; do
                validate_service_name "$svc"
            done
            shift 2
            ;;
        --stop)
            ACTION="stop"
            shift
            ;;
        --status)
            ACTION="status"
            shift
            ;;
        --help|-h)
            ACTION="help"
            shift
            ;;
        *)
            log_error "未知选项: $1"
            log_error "使用 --help 查看帮助"
            exit 1
            ;;
    esac
done

# ========== 执行 ==========

case "$ACTION" in
    start)  do_start  ;;
    stop)   do_stop   ;;
    status) do_status ;;
    help)   do_help   ;;
esac
