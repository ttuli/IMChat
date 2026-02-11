package service

import "context"

type AuthService interface {
	GetAuthCode(ctx context.Context, req *GetAuthCodeRequest) (*GetAuthCodeResponse, error)
	Register(ctx context.Context, req *RegisterRequest) (*RegisterResponse, error)
	Login(ctx context.Context, req *LoginRequest) (*LoginResponse, error)
	Logout(ctx context.Context, req *LogoutRequest) (*LogoutResponse, error)
	Refresh(ctx context.Context, req *RefreshReq) (*RefreshResp, error)
}