package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"IM2/pkg/resultx"

	"github.com/google/uuid"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// APISIXConfig APISIX 注册配置
type APISIXConfig struct {
	Enable          bool   `json:",default=false"`          // 是否启用 APISIX 注册
	AdminURL        string `json:",optional"`               // APISIX Admin API 地址，如 http://127.0.0.1:9180
	APIKey          string `json:",optional"`               // APISIX Admin API Key
	UpstreamID      string `json:",optional"`               // 上游 ID
	RouteID         string `json:",optional"`               // 路由 ID（可选，不设置则uuid生成）
	RouteURI        string `json:",optional"`               // 路由 URI，如 /api/user/*
	ServiceIP       string `json:",optional"`               // 当前服务对外暴露的 IP（可选，默认使用 Host）
	EnableWebsocket bool   `json:",optional,default=false"` // 是否启用 WebSocket
}

// RestConfExtractor 从配置中提取 RestConf
type RestConfExtractor func(cfg any) *rest.RestConf

// APISIXExtractor 从配置中提取 APISIX 配置
type APISIXExtractor func(cfg any) *APISIXConfig

// RestService 通用的 REST API 服务实现
type RestService struct {
	registerServices  RegisterRestServicesFunc
	server            *rest.Server      // rest 服务器
	restConf          *rest.RestConf    // rest 配置
	apisixCfg         *APISIXConfig     // APISIX 配置
	nodeAddr          string            // 注册到 APISIX 的节点地址
	restConfExtractor RestConfExtractor // rest 配置提取器
	apisixExtractor   APISIXExtractor   // APISIX 配置提取器
}

type RegisterRestServicesFunc func(cfg any, server *rest.Server) error

// RestOption 函数选项类型
type RestOption func(*RestService)

// WithRestConf 设置 REST 配置提取器
func WithRestConf(extractor RestConfExtractor) RestOption {
	return func(rs *RestService) {
		rs.restConfExtractor = extractor
	}
}

// WithAPISIX 设置 APISIX 配置提取器
func WithAPISIX(extractor APISIXExtractor) RestOption {
	return func(rs *RestService) {
		rs.apisixExtractor = extractor
	}
}

// NewRestService 创建通用的 RestService
func NewRestService(registerServices RegisterRestServicesFunc, opts ...RestOption) *RestService {
	rs := &RestService{
		registerServices: registerServices,
	}
	for _, opt := range opts {
		opt(rs)
	}
	return rs
}

// Load 加载配置并初始化服务
func (rs *RestService) Load(cfg any) error {
	// 使用提取器从 cfg 中提取配置
	if rs.restConfExtractor != nil {
		rs.restConf = rs.restConfExtractor(cfg)
	}
	if rs.apisixExtractor != nil {
		rs.apisixCfg = rs.apisixExtractor(cfg)
	}

	if rs.restConf == nil {
		return fmt.Errorf("REST 配置不能为空，请使用 WithRestConf 设置提取器")
	}

	server, err := rest.NewServer(*rs.restConf, rest.WithCors("*"))
	if err != nil {
		return fmt.Errorf("创建 rest 服务器失败: %w", err)
	}
	rs.server = server

	if err := rs.registerServices(cfg, server); err != nil {
		return fmt.Errorf("注册服务失败: %w", err)
	}

	// 初始化 APISIX 节点地址
	if rs.apisixCfg != nil && rs.apisixCfg.Enable {
		serviceIP := rs.apisixCfg.ServiceIP
		if serviceIP == "" {
			serviceIP = rs.restConf.Host
		}
		rs.nodeAddr = fmt.Sprintf("%s:%d", serviceIP, rs.restConf.Port)
		if rs.apisixCfg.RouteID == "" {
			rs.apisixCfg.RouteID = uuid.New().String()
		}
	}

	// 设置全局响应处理器
	httpx.SetOkHandler(resultx.OkHandler)
	httpx.SetErrorHandlerCtx(resultx.ErrorHandler)

	return nil
}

// Start 启动服务
func (rs *RestService) Start() error {
	if rs.server == nil {
		return fmt.Errorf("服务器未初始化，请先调用 Load 方法")
	}

	// 注册到 APISIX
	if err := rs.registerToAPISIX(); err != nil {
		return fmt.Errorf("注册到 APISIX 失败: %w", err)
	}

	// 在 goroutine 中启动，避免阻塞
	go func() {
		rs.server.Start()
	}()
	return nil
}

// Stop 停止服务
func (rs *RestService) Stop() error {
	// 先从 APISIX 注销
	if err := rs.deregisterFromAPISIX(); err != nil {
		fmt.Printf("从 APISIX 注销失败: %v\n", err)
	}

	if rs.server != nil {
		rs.server.Stop()
	}
	return nil
}

// registerToAPISIX 注册服务节点到 APISIX upstream
func (rs *RestService) registerToAPISIX() error {
	if rs.apisixCfg == nil || !rs.apisixCfg.Enable {
		return nil
	}

	url := fmt.Sprintf("%s/apisix/admin/upstreams/%s",
		rs.apisixCfg.AdminURL, rs.apisixCfg.UpstreamID)

	// 创建或更新 upstream，使用完整配置
	payload := map[string]any{
		"type": "roundrobin", // 负载均衡算法
		"nodes": map[string]int{
			rs.nodeAddr: 1,
		},
		"scheme":           "http", // 协议类型
	}

	body, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-KEY", rs.apisixCfg.APIKey)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("APISIX upstream 注册失败，状态码: %d", resp.StatusCode)
	}

	fmt.Printf("成功注册 upstream 到 APISIX: %s\n", rs.nodeAddr)

	// 注册路由
	if err := rs.registerRouteToAPISIX(); err != nil {
		return fmt.Errorf("注册路由失败: %w", err)
	}
	return nil
}

// deregisterFromAPISIX 从 APISIX upstream 注销服务节点
func (rs *RestService) deregisterFromAPISIX() error {
	if rs.apisixCfg == nil || !rs.apisixCfg.Enable {
		return nil
	}

	url := fmt.Sprintf("%s/apisix/admin/upstreams/%s",
		rs.apisixCfg.AdminURL, rs.apisixCfg.UpstreamID)

	// 1. 先获取当前 upstream 的配置
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-KEY", rs.apisixCfg.APIKey)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// 如果 upstream 不存在，说明已经被删除了，直接返回成功
		if resp.StatusCode == http.StatusNotFound {
			fmt.Printf("Upstream %s 不存在，跳过注销\n", rs.apisixCfg.UpstreamID)
			return nil
		}
		return fmt.Errorf("获取 upstream 失败，状态码: %d", resp.StatusCode)
	}

	// 2. 解析响应
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("解析响应失败: %w", err)
	}

	// 3. 提取 value.nodes
	value, ok := result["value"].(map[string]any)
	if !ok {
		return fmt.Errorf("响应格式错误: value 字段不存在或格式错误")
	}

	nodes, ok := value["nodes"].(map[string]any)
	if !ok {
		nodes = make(map[string]any)
	}

	// 4. 删除当前节点
	delete(nodes, rs.nodeAddr)

	// 5. 如果节点列表为空，可以选择删除整个 upstream 或保留空的 upstream
	// 这里选择保留 upstream，但将 nodes 设为空 map
	payload := map[string]any{
		"type":   value["type"],   // 保留负载均衡类型
		"scheme": value["scheme"], // 保留协议
		"nodes":  nodes,
	}

	// 6. 更新 upstream
	body, _ := json.Marshal(payload)
	req, err = http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-KEY", rs.apisixCfg.APIKey)

	resp, err = client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("APISIX 注销失败，状态码: %d", resp.StatusCode)
	}

	fmt.Printf("成功从 APISIX 注销: %s\n", rs.nodeAddr)
	return nil
}

// registerRouteToAPISIX 注册路由到 APISIX
func (rs *RestService) registerRouteToAPISIX() error {
	// 如果没有配置 RouteID，则跳过路由注册
	if rs.apisixCfg.RouteID == "" || rs.apisixCfg.RouteURI == "" {
		return nil
	}

	url := fmt.Sprintf("%s/apisix/admin/routes/%s",
		rs.apisixCfg.AdminURL, rs.apisixCfg.RouteID)

	// 创建路由配置
	payload := map[string]any{
		"uri":              rs.apisixCfg.RouteURI,
		"upstream_id":      rs.apisixCfg.UpstreamID,
		"name":             rs.apisixCfg.RouteID,
		"status":           1, // 启用状态
		"enable_websocket": rs.apisixCfg.EnableWebsocket,
	}

	body, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-KEY", rs.apisixCfg.APIKey)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("APISIX 路由注册失败，状态码: %d", resp.StatusCode)
	}

	fmt.Printf("成功注册路由到 APISIX: %s -> %s\n", rs.apisixCfg.RouteURI, rs.apisixCfg.UpstreamID)
	return nil
}
