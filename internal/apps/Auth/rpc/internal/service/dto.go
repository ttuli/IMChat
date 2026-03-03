package service

type GetAuthCodeRequest struct {
	Phone string
}

type GetAuthCodeResponse struct {
}

type LoginRequest struct {
	Account  uint64
	Password string
	DeviceID string
	Platform string
}

type LoginResponse struct {
	Token        string
	RefreshToken string
}

type LogoutRequest struct {
	UserID   uint64
	Platform string
	RemoveRT bool
}

type LogoutResponse struct {
}

type RegisterRequest struct {
	Name     string
	Password string
	Phone    string
	AuthCode string
}

type RegisterResponse struct {
	ID uint64
}

type RefreshReq struct {
	DeviceId     string
	Platform     string
	RefreshToken string
}

type RefreshResp struct {
	Token        string
	RefreshToken string
}
