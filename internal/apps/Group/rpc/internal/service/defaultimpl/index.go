package defaultimpl

import (
	"IM2/interceptor"
	"IM2/internal/apps/Group/rpc/config"
	"IM2/internal/apps/Group/rpc/internal/dao"
	"IM2/internal/apps/Group/rpc/internal/service"
	"IM2/internal/apps/Idgen/rpc/idgenclient"

	"github.com/zeromicro/go-zero/zrpc"
)

// groupService 群组服务实现
type groupService struct {
	config      config.Config
	groupDAO    *dao.GroupDAO
	applyDAO    *dao.ApplyDAO
	idGenerator idgenclient.Idgen
}

// NewGroupService 创建群组服务
func NewGroupService(c config.Config) service.GroupService {
	return &groupService{
		config:   c,
		groupDAO: dao.NewGroupDAO(c.DAO.GroupDAO.DataSource, c.DAO.GroupDAO.RedisSource),
		applyDAO: dao.NewApplyDAO(c.DAO.ApplyDAO),
		idGenerator: idgenclient.NewIdgen(zrpc.MustNewClient(c.IDRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientPureErrorInterceptor))),
	}
}
