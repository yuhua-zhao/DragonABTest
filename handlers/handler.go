package handlers

import (
	"context"

	"github.com/yuhua-zhao/DragonABTest/service"

	"github.com/FlyDragonGO/DragonPlusServerUtils/logging"
	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	pb "github.com/FlyDragonGO/ProtobufDefinition/go/grpc/abtest"
)

type Handler struct {
}

var logger = logging.NewLogger("abtest", &logging.LogOptions{
	// KinesisStreamName: os.Getenv("KINESIS_STREAM_NAME"),
	// KinesisRegion:     os.Getenv("KINESIS_REGION"),
	MaxSize:    16,
	MaxBackups: 3,
})

func (*Handler) GetABTests(ctx context.Context, req *pb.GetABTestRequest) (*pb.GetABTestResponse, error) {
	logger.WithFields(map[string]interface{}{
		"app_name": req.App,
		"action":   "list_abtests",
		"status":   req.Status,
	}).Info("")
	results, count, err := service.ListABTests(req.App, req.Status, int(req.Limit), int(req.Offset))
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
	_, err := service.CreateABTest(req.Item)
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
	abtestItem, err := service.UpdateABTest(req.Item)

	if err != nil {
		return &pb.UpdateABTestResponse{
			Status: &pb.CommonStatus{
				IsOk: false,
				Msg:  err.Error(),
			},
		}, err
	}
	return &pb.UpdateABTestResponse{
		Status: &pb.CommonStatus{
			IsOk: true,
			Msg:  "",
		},
		Item: abtestItem,
	}, nil
}

func (*Handler) DeleteABTest(ctx context.Context, req *pb.DeleteABTestRequest) (*pb.DeleteABTestResponse, error) {
	service.TransABTestStatus(req.AbtestId, abtest.ABTestStatus_DELETED)
	return &pb.DeleteABTestResponse{}, nil
}

func (*Handler) GetABTestConfigByPersona(ctx context.Context, req *pb.GetABTestConfigRequest) (*pb.GetABTestConfigResponse, error) {
	abtestMap, err := service.GenerateABTestConfigByPersonas(req.Personas, req.Header)
	if err != nil {
		return nil, err
	}

	return &pb.GetABTestConfigResponse{AbtestConfig: abtestMap}, nil
}
