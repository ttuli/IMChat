package svc

import (
	"fmt"
	"hash/crc32"
	"os"
	"strconv"

	"IM2/internal/apps/Group/rpc/client/grouprpc"
	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/dao"
	"IM2/internal/apps/Message/rpc/internal/members"
	"IM2/internal/apps/Message/rpc/internal/seq"
	"IM2/internal/interceptor"
	"IM2/pkg/redisx"

	"github.com/bwmarrin/snowflake"
	"github.com/nats-io/nats.go"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config        config.Config
	MessageDAO    *dao.MessageDAO
	SessionDAO    *dao.SessionDAO
	NatsConn      *nats.Conn
	Js            nats.JetStreamContext
	Redis         *redisx.Client
	SnowflakeNode *snowflake.Node
	SeqAllocator  *seq.Allocator
	// Members 群成员来源（Redis 缓存 + Group RPC 回源），群消息扇出与时间线更新共用
	Members *members.Provider
}

func NewServiceContext(c config.Config) *ServiceContext {
	if c.DAO.MessageDAO.UnreadCountLimit == 0 {
		c.DAO.MessageDAO.UnreadCountLimit = 100
	}

	conn, err := nats.Connect(c.Listener.Url)
	if err != nil {
		panic(err)
	}
	js, err := conn.JetStream()
	if err != nil {
		panic(err)
	}

	redisClient, err := redisx.NewClient(c.DAO.SessionDAO.Redisx)
	if err != nil {
		panic(err)
	}

	// 初始化本地雪花发号器
	nodeID := c.SnowflakeNodeID
	if nodeID == 0 {
		if envNodeID := os.Getenv("NODE_ID"); envNodeID != "" {
			if id, err := strconv.ParseInt(envNodeID, 10, 64); err == nil {
				nodeID = id
			}
		}
	}
	if nodeID == 0 {
		hostname, _ := os.Hostname()
		nodeID = int64(crc32.ChecksumIEEE([]byte(fmt.Sprintf("msg-%s", hostname))) % 1024)
	}
	sfNode, sfErr := snowflake.NewNode(nodeID)
	if sfErr != nil {
		panic(fmt.Sprintf("init message snowflake node failed: %v", sfErr))
	}

	msgDao := dao.NewMessageDAO(c.DAO.MessageDAO)
	ssDao := dao.NewSessionDAO(c.DAO.SessionDAO.Dbsource, c.DAO.SessionDAO.Redisx)

	groupRpc := grouprpc.NewGroupRpc(zrpc.MustNewClient(c.GroupRpc,
		zrpc.WithUnaryClientInterceptor(interceptor.ClientPureErrorInterceptor)))
	memberProvider := members.NewProvider(redisClient, groupRpc)
	// SeqSyncer 更新群会话时间线时经此获取成员，避免按 user_session 表反查（含退群用户）
	ssDao.SetGroupMemberSource(memberProvider.GetMemberIDs)

	return &ServiceContext{
		Config:        c,
		NatsConn:      conn,
		Js:            js,
		MessageDAO:    msgDao,
		SessionDAO:    ssDao,
		Redis:         redisClient,
		SnowflakeNode: sfNode,
		// Lamport seq 分配器与雪花发号器共用节点 ID（0-1023）
		SeqAllocator: seq.NewAllocator(nodeID),
		Members:      memberProvider,
	}
}
