package defaultimpl

import (
	"IM2/interceptor"
	"IM2/internal/apps/Idgen/rpc/idgenclient"
	"IM2/internal/apps/User/rpc/config"
	"IM2/internal/apps/User/rpc/internal/dao"
	"IM2/internal/apps/User/rpc/internal/service"

	"github.com/zeromicro/go-zero/zrpc"
)

// userService 用户服务实现
type userService struct {
	config.Config
	userDAO        *dao.UserDAO
	friendDAO      *dao.FriendDAO
	friendApplyDAO *dao.FriendApplyDAO
	idGenerator    idgenclient.Idgen
}

// NewUserService 创建用户服务
func NewUserService(c config.Config) service.UserService {
	return &userService{
		userDAO:        dao.NewUserDAO(c.DAO.UserDAO.DataSource, c.DAO.UserDAO.RedisSource),
		friendDAO:      dao.NewFriendDAO(c.DAO.FriendDAO),
		friendApplyDAO: dao.NewFriendApplyDAO(c.DAO.FriendApplyDAO),
		idGenerator: idgenclient.NewIdgen(zrpc.MustNewClient(c.IDRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientPureErrorInterceptor))),
		Config: c,
	}
}
