package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"IM2/interceptor"
	"IM2/internal/apps/Auth/rpc/config"
	userclient "IM2/internal/apps/User/rpc/client/user"
	"IM2/internal/apps/User/rpc/user"
	"IM2/pkg/logger"
	"IM2/pkg/redisc"
	tokenmanager "IM2/pkg/tokenManager"
	"IM2/pkg/xerr"

	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	dypnsapi20170525 "github.com/alibabacloud-go/dypnsapi-20170525/v3/client"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
	credential "github.com/aliyun/credentials-go/credentials"
	"github.com/golang-jwt/jwt/v4"
	"github.com/zeromicro/go-zero/zrpc"
)

type AuthServiceImpl struct {
	userclient.User
	*redisc.RedisModel
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

func NewAuthServiceImpl(c config.Config) *AuthServiceImpl {
	client, err := createClient()
	if err != nil {
		panic(err)
	}
	return &AuthServiceImpl{
		User: userclient.NewUser(zrpc.MustNewClient(c.UserRpc,
			zrpc.WithUnaryClientInterceptor(
				interceptor.ClientPureErrorInterceptor),
		)),
		RedisModel:     redisc.MustNewRedis(c.Redisx),
		TokenManager:   tokenmanager.NewTokenManager(c.TokenConfig),
		DypnsapiClient: client,
	}
}

func (s *AuthServiceImpl) GetAuthCode(ctx context.Context, req *GetAuthCodeRequest) (*GetAuthCodeResponse, error) {
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
		return nil, xerr.New(xerr.ErrInternalServer, "获取验证码失败")
	}
	return &GetAuthCodeResponse{}, nil
}

func (s *AuthServiceImpl) verifyAuthCode(phone, code string) bool {
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

func (s *AuthServiceImpl) Register(ctx context.Context, req *RegisterRequest) (*RegisterResponse, error) {
	resp := &RegisterResponse{}

	// 1. 验证验证码
	if !s.verifyAuthCode(req.Phone, req.AuthCode) {
		return nil, xerr.New(xerr.ErrAuthCodeError, "验证码错误")
	}

	// 2. 创建用户
	response, err := s.User.CreateUser(ctx, &user.CreateUserReq{
		Name:              req.Name,
		Password:          req.Password,
		Phone:             req.Phone,
		Gender:            1,
		JoinType:          1,
		Avatar:            "",
		PersonalSignature: "",
	})
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "注册失败")
	}
	resp.ID = response.UserId

	return resp, nil
}

func (s *AuthServiceImpl) Login(ctx context.Context, req *LoginRequest) (*LoginResponse, error) {
	resp := &LoginResponse{}

	valid, err := s.User.VerifyPassword(ctx, &user.VerifyPasswordReq{
		UserId:   req.Account,
		Password: req.Password,
	})
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "登陆失败,请检查账户密码")
	}
	if !valid.Valid {
		return nil, xerr.New(xerr.ErrPasswordError, "密码错误")
	}

	// 3. 生成token
	accessToken, err := s.TokenManager.GenerateJWTToken(req.Account, tokenmanager.AccessToken, nil)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrTokenGenerate, "登录失败")
	}
	resp.Token = accessToken

	refreshToken, err := s.TokenManager.GenerateJWTToken(req.Account, tokenmanager.RefreshToken, jwt.MapClaims{
		tokenmanager.ClaimKeyPlatform: req.Platform,
		tokenmanager.ClaimKeyDeviceID: req.DeviceID,
	})
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrTokenGenerate, "登录失败")
	}
	resp.RefreshToken = refreshToken

	return resp, nil
}

func (s *AuthServiceImpl) Logout(ctx context.Context, req *LogoutRequest) (*LogoutResponse, error) {
	// 同时删除 access token 和 refresh token
	err := s.TokenManager.InvalidateTokenByUserID(req.UserID, req.Platform, req.RemoveRT)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrTokenGenerate, "登出失败")
	}
	return &LogoutResponse{}, nil
}

func (s *AuthServiceImpl) Refresh(ctx context.Context, req *RefreshReq) (*RefreshResp, error) {
	resp := &RefreshResp{}

	// 1. 验证refresh token
	id, err := s.TokenManager.ValidateToken(ctx, req.RefreshToken)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrTokenGenerate, "刷新token失败")
	}
	if id == 0 {
		return nil, xerr.New(xerr.ErrTokenGenerate, "refresh token无效")
	}

	// 2. 生成新的tokenid
	accessToken, err := s.TokenManager.GenerateJWTToken(id, tokenmanager.AccessToken, nil)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrTokenGenerate, "刷新token失败")
	}
	resp.Token = accessToken

	// 3. 生成新的refresh token
	refreshToken, err := s.TokenManager.GenerateJWTToken(id, tokenmanager.RefreshToken, jwt.MapClaims{
		tokenmanager.ClaimKeyPlatform: req.Platform,
		tokenmanager.ClaimKeyDeviceID: req.DeviceId,
	})
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrTokenGenerate, "刷新token失败")
	}
	resp.RefreshToken = refreshToken

	return resp, nil
}
