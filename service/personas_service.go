package service

import (
	"fmt"
	"os"
	"sync"

	pb "github.com/FlyDragonGO/ProtobufDefinition/go/grpc/personas"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PersonasGRPCClient struct {
	Conn   *grpc.ClientConn
	Client pb.PersonasServiceClient
}

var (
	singleInstance *PersonasGRPCClient
	once           sync.Once
)

func GetPersonasGrpcClient() *PersonasGRPCClient {
	once.Do(func() {
		var addr string
		switch os.Getenv("PROCESS_ENV") {
		case "production":
			addr = "..."
		case "develop":
			addr = ""
		case "local":
			addr = "127.0.0.1:8080"
		}
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Println(err)
		}

		grpcClient := pb.NewPersonasServiceClient(conn)
		singleInstance = &PersonasGRPCClient{
			Conn:   conn,
			Client: grpcClient,
		}
	})
	return singleInstance
}
