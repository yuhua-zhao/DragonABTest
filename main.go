package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/yuhua-zhao/DragonABTest/handlers"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.elastic.co/apm/module/apmgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/FlyDragonGO/ProtobufDefinition/go/grpc/abtest"
)

var (
	handler = &handlers.Handler{}
	grpcSrv *grpc.Server
)

func metricsStart() {
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}

func grpcStart() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcSrv = grpc.NewServer(grpc.UnaryInterceptor(apmgrpc.NewUnaryServerInterceptor(apmgrpc.WithRecovery())))
	// pb.RegisterPersonasServiceServer(grpcSrv, handler)
	pb.RegisterABTestServiceServer(grpcSrv, handler)
	reflection.Register(grpcSrv)
	err = grpcSrv.Serve(lis)
	if err != nil {
		panic(err)
	}
}

func grpcStop() {
	grpcSrv.GracefulStop()
}

func waitForInterrupt() {
	var signalChannel chan os.Signal
	signalChannel = make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt)
	<-signalChannel

	// stop grpc handlers
	grpcStop()
}

func main() {
	// metrics
	go metricsStart()

	// grpc handlers
	go grpcStart()

	timer := time.AfterFunc(time.Second*1, func() {
		fmt.Println("timeout")
	})
	defer timer.Stop()

	waitForInterrupt()
}
