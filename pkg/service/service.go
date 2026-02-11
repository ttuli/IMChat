package service

type Service interface {
	// 务必是非阻塞函数
	Start() error

	Stop() error
	
	Load(config any) error
}