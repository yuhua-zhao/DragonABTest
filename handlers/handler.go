package handlers

import (
	"context"
	"os"

	"github.com/yuhua-zhao/DragonABTest/service"

	"github.com/FlyDragonGO/DragonPlusServerUtils/logging"
	pb "github.com/FlyDragonGO/ProtobufDefinition/go/grpc/abtest"
)

type Handler struct {
}

var logger = logging.NewLogger("abtest", &logging.LogOptions{
	KinesisStreamName: os.Getenv("KINESIS_STREAM_NAME"),
	KinesisRegion:     os.Getenv("KINESIS_REGION"),
	MaxSize:           16,
	MaxBackups:        3,
})

func (*Handler) GetABTests(ctx context.Context, req *pb.GetABTestRequest) (*pb.GetABTestResponse, error) {
	results, count, err := service.ListABTests(req.App, int(req.Limit), int(req.Offset), req.Status)
	if err != nil {
		return &pb.GetABTestResponse{
			Status: &pb.CommonStatus{
				IsOk: false,
				Msg:  err.Error(),
			},
			Items: nil,
			Total: 0,
		}, err
	} else {
		return &pb.GetABTestResponse{
			Status: &pb.CommonStatus{
				IsOk: true,
				Msg:  "",
			},
			Items: results,
			Total: uint32(count),
		}, nil
	}
}

func (*Handler) CreateABTest(ctx context.Context, req *pb.CreateABTestRequest) (*pb.CreateABTestResponse, error) {
	logger.WithFields(map[string]interface{}{
		"action": "read_error",
		"app":    req.Item.App,
		"abtest": req.Item,
	}).Info("")
	_, err := service.UpsertABTest(req.Item)
	if err == nil {
		return &pb.CreateABTestResponse{
			Status: &pb.CommonStatus{
				IsOk: true,
				Msg:  "",
			},
		}, nil
	} else {
		return &pb.CreateABTestResponse{
			Status: &pb.CommonStatus{
				IsOk: false,
				Msg:  err.Error(),
			},
		}, err
	}
}

func (*Handler) UpdateABTest(ctx context.Context, req *pb.UpdateABTestRequest) (*pb.UpdateABTestResponse, error) {
	logger.WithFields(map[string]interface{}{
		"action": "read_error",
		"app":    req.Item.App,
		"abtest": req.Item,
	}).Info("")

	return &pb.UpdateABTestResponse{}, nil
}

func (*Handler) DeleteABTest(ctx context.Context, req *pb.DeleteABTestRequest) (*pb.DeleteABTestResponse, error) {
	logger.WithFields(map[string]interface{}{
		"action": "read_error",
		"app":    req.App,
		"abtest": req.AbtestId,
	}).Info("")
	return &pb.DeleteABTestResponse{}, nil
}

func (*Handler) GetPersonasABTestConfig(ctx context.Context, req *pb.GetABTestConfigRequest) (*pb.GetABTestConfigResponse, error) {
	logger.WithFields(map[string]interface{}{
		"action": "read_error",
	}).Info("")
	return &pb.GetABTestConfigResponse{}, nil
}
