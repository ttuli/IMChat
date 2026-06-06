package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	model "IM2/internal/Entity"
	"IM2/internal/apps/Auth/rpc/config"
	"IM2/internal/apps/Auth/rpc/internal/dao"
	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"
	tokenmanager "IM2/pkg/tokenManager"
	"IM2/pkg/xerr"

	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	dypnsapi20170525 "github.com/alibabacloud-go/dypnsapi-20170525/v3/client"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
	credential "github.com/aliyun/credentials-go/credentials"
)

type AuthService struct {
	AuthDAO        *dao.AuthDAO
	TokenManager   *tokenmanager.TokenManager
	DypnsapiClient *dypnsapi20170525.Client
}

func createClient() (_result *dypnsapi20170525.Client, _err error) {
	// 工程代码建议使用更安全的无AK方式，凭据配置方式请参见：https://help.aliyun.com/document_detail/378661.html。
	credential, _err := credential.NewCredential(nil)
	if _err != nil {
		return _result, _err
	}

	config := &openapi.Config{
		Credential: credential,
	}
	// Endpoint 请参考 https://api.aliyun.com/product/Dypnsapi
	config.Endpoint = tea.String("dypnsapi.aliyuncs.com")
	_result = &dypnsapi20170525.Client{}
	_result, _err = dypnsapi20170525.NewClient(config)
	return _result, _err
}

func NewAuthService(c config.Config) *AuthService {
	client, err := createClient()
	if err != nil {
		panic(err)
	}

	return &AuthService{
		AuthDAO:        dao.NewAuthDAO(c.AuthDAO),
		TokenManager:   tokenmanager.NewTokenManager(c.TokenConfig),
		DypnsapiClient: client,
	}
}

func (s *AuthService) GetAuthCode(ctx context.Context, req *GetAuthCodeRequest) (*GetAuthCodeResponse, error) {
	sendSmsVerifyCodeRequest := &dypnsapi20170525.SendSmsVerifyCodeRequest{
		SignName:      tea.String("速通互联验证码"),
		TemplateCode:  tea.String("100001"),
		PhoneNumber:   tea.String(req.Phone),
		TemplateParam: tea.String("{\"code\":\"##code##\",\"min\":\"5\"}"),
	}
	runtime := &util.RuntimeOptions{}
	tryErr := func() (_e error) {
		defer func() {
			if r := tea.Recover(recover()); r != nil {
				_e = r
			}
		}()
		resp, _err := s.DypnsapiClient.SendSmsVerifyCodeWithOptions(sendSmsVerifyCodeRequest, runtime)
		if _err != nil {
			return _err
		}

		logger.Infof("[LOG] %v\n", resp)

		return nil
	}()

	if tryErr != nil {
		var error = &tea.SDKError{}
		if _t, ok := tryErr.(*tea.SDKError); ok {
			error = _t
		} else {
			error.Message = tea.String(tryErr.Error())
		}
		logger.Errorf("get auth code error %v\n", tea.StringValue(error.Message))
		// 诊断地址
		var data interface{}
		d := json.NewDecoder(strings.NewReader(tea.StringValue(error.Data)))
		d.Decode(&data)
		if m, ok := data.(map[string]interface{}); ok {
			recommend, _ := m["Recommend"]
			logger.Errorf("get auth code error %v\n", recommend)
		}
		return nil, xerr.New(transport.ErrorCode_ERR_INTERNAL_SERVER, "获取验证码失败")
	}
	return &GetAuthCodeResponse{}, nil
}

func (s *AuthService) verifyAuthCode(phone, code string) bool {
	checkSmsVerifyCodeRequest := &dypnsapi20170525.CheckSmsVerifyCodeRequest{
		PhoneNumber: tea.String(phone),
		VerifyCode:  tea.String(code),
	}
	runtime := &util.RuntimeOptions{}
	tryErr := func() (_e error) {
		defer func() {
			if r := tea.Recover(recover()); r != nil {
				_e = r
			}
		}()
		resp, _err := s.DypnsapiClient.CheckSmsVerifyCodeWithOptions(checkSmsVerifyCodeRequest, runtime)
		if _err != nil {
			return _err
		}

		fmt.Printf("[LOG] %v\n", resp)

		return nil
	}()

	if tryErr != nil {
		var error = &tea.SDKError{}
		if _t, ok := tryErr.(*tea.SDKError); ok {
			error = _t
		} else {
			error.Message = tea.String(tryErr.Error())
		}

		logger.Error(tea.StringValue(error.Message))
		// 诊断地址
		var data interface{}
		d := json.NewDecoder(strings.NewReader(tea.StringValue(error.Data)))
		d.Decode(&data)
		if m, ok := data.(map[string]interface{}); ok {
			recommend, _ := m["Recommend"]
			logger.Errorf("get auth code error %v\n", recommend)
		}
		return false
	}
	return true
}

func (s *AuthService) Register(ctx context.Context, req *RegisterRequest) (*RegisterResponse, error) {
	resp := &RegisterResponse{}

	// 1. 验证验证码
	if !s.verifyAuthCode(req.Phone, req.AuthCode) {
		return nil, xerr.New(transport.ErrorCode_ERR_AUTH_CODE_ERROR, "验证码错误")
	}

	// 2. 构建用户对象（业务规则由充血模型 NewUser 封装：密码哈希、字段初始值）
	u, err := model.NewUser(req.Name, req.Password, req.Phone)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_INTERNAL_SERVER, "创建用户失败")
	}

	// 3. 持久化（AuthDAO 只负责写入，不含业务判断）
	if err := s.AuthDAO.InsertUser(ctx, u); err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "注册失败")
	}
	resp.ID = u.UserID

	return resp, nil
}

func (s *AuthService) Login(ctx context.Context, req *LoginRequest) (*LoginResponse, error) {
	resp := &LoginResponse{}

	// 1. 查询用户（AuthDAO 只负责查询，不含业务判断）
	// req.Account 为用户ID（uint64），按 ID 查询
	u, err := s.AuthDAO.FindOneByID(ctx, req.Account)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "登陆失败,请检查账户密码")
	}

	// 2. 验证密码（业务规则由充血模型 VerifyPassword 封装）
	if !u.VerifyPassword(req.Password) {
		return nil, xerr.New(transport.ErrorCode_ERR_PASSWORD_ERROR, "密码错误")
	}

	// 3. 通过 OnLogin 统一完成：
	//    - 写 session:{id}（无 TTL），获取 version
	//    - 生成携带 ver claim 的 AT
	//    - 生成 RT（remember_me=true 时同时持久化到 Redis）
	accessToken, refreshToken, err := s.TokenManager.OnLogin(ctx, req.Account, tokenmanager.LoginOptions{
		// DeviceID 即为登录机器标识（machine_id）
		MachineID:  req.DeviceID,
		RememberMe: req.RemeberMe,
	})
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_TOKEN_GENERATE, "登录失败")
	}
	resp.Token = accessToken
	resp.RefreshToken = refreshToken

	return resp, nil
}

func (s *AuthService) Refresh(ctx context.Context, req *RefreshReq) (*RefreshResp, error) {
	resp := &RefreshResp{}

	// 1. 解析 RT 获取 userID 和 deviceID（不校验过期，签名合法即可）
	userID, deviceID, _, err := s.TokenManager.ParseTokenInfo(req.RefreshToken)
	if err != nil || userID == 0 {
		return nil, xerr.New(transport.ErrorCode_ERR_TOKEN_GENERATE, "refresh token 无效")
	}

	// 2. 校验请求设备是否与 session 中的 machine_id 一致
	//    session.machine_id != req.DeviceId 说明是其他设备在刷 RT，拒绝
	session, err := s.TokenManager.GetSession(ctx, userID)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_TOKEN_GENERATE, "刷新token失败")
	}
	if session.MachineID != req.DeviceId {
		return nil, xerr.New(transport.ErrorCode_ERR_KICKED_OUT, "设备不匹配，拒绝刷新")
	}

	// 3. 检查 Redis 中是否存在 rt 键，决定是否为 remember_me 路径：
	//    - rt 键存在：登录时勾选了 remember_me（一键登录），刷新时同步更新 RT 键
	//    - rt 键不存在：普通登录，不写 RT 键，避免干扰登录时的 session 键值
	rememberMe, err := s.TokenManager.HasRefreshToken(ctx, userID, deviceID)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_TOKEN_GENERATE, "刷新token失败")
	}

	// 4. 通过 OnLogin 续期 session（version+1）并重新生成 AT/RT
	accessToken, refreshToken, err := s.TokenManager.OnLogin(ctx, userID, tokenmanager.LoginOptions{
		MachineID:  req.DeviceId,
		RememberMe: rememberMe,
	})
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_TOKEN_GENERATE, "刷新token失败")
	}
	resp.Token = accessToken
	resp.RefreshToken = refreshToken

	return resp, nil
}
