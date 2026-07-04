package svc

import (
	"fmt"
	"hash/crc32"
	"os"
	"strconv"

	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/dao"
	"IM2/pkg/redisx"

	"github.com/bwmarrin/snowflake"
	"github.com/nats-io/nats.go"
)

type ServiceContext struct {
	Config        config.Config
	MessageDAO    *dao.MessageDAO
	SessionDAO    *dao.SessionDAO
	NatsConn      *nats.Conn
	Js            nats.JetStreamContext
	Redis         *redisx.Client
	SnowflakeNode *snowflake.Node
}

func NewServiceContext(c config.Config) *ServiceContext {
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

	msgDao := dao.NewMessageDAO(c.DAO.MessageDAO.Dbsource)
	ssDao := dao.NewSessionDAO(c.DAO.SessionDAO.Dbsource, c.DAO.SessionDAO.Redisx)

	return &ServiceContext{
		Config:        c,
		NatsConn:      conn,
		Js:            js,
		MessageDAO:    msgDao,
		SessionDAO:    ssDao,
		Redis:         redisClient,
		SnowflakeNode: sfNode,
	}
}
