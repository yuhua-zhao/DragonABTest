package handlers

import (
	"context"

	"github.com/yuhua-zhao/DragonABTest/service"

	"github.com/FlyDragonGO/DragonPlusServerUtils/logging"
	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	abtest_grpc "github.com/FlyDragonGO/ProtobufDefinition/go/grpc/abtest"
	personas_grpc "github.com/FlyDragonGO/ProtobufDefinition/go/grpc/personas"
)

type Handler struct {
}

var logger = logging.NewLogger("abtest", &logging.LogOptions{
	// KinesisStreamName: os.Getenv("KINESIS_STREAM_NAME"),
	// KinesisRegion:     os.Getenv("KINESIS_REGION"),
	MaxSize:    16,
	MaxBackups: 3,
})

func (*Handler) GetABTests(ctx context.Context, req *abtest_grpc.GetABTestRequest) (*abtest_grpc.GetABTestResponse, error) {
	logger.WithFields(map[string]interface{}{
		"app_name": req.App,
		"action":   "list_abtests",
		"status":   req.Status,
	}).Info("")
	results, count, err := service.ListABTests(req.App, req.Status, int(req.Limit), int(req.Offset))
	if err != nil {
		return &abtest_grpc.GetABTestResponse{
			Status: &abtest_grpc.CommonStatus{
				IsOk: false,
				Msg:  err.Error(),
			},
			Items: nil,
			Total: 0,
		}, err
	} else {
		return &abtest_grpc.GetABTestResponse{
			Status: &abtest_grpc.CommonStatus{
				IsOk: true,
				Msg:  "",
			},
			Items: results,
			Total: uint32(count),
		}, nil
	}
}

func (*Handler) CreateABTest(ctx context.Context, req *abtest_grpc.CreateABTestRequest) (*abtest_grpc.CreateABTestResponse, error) {
	_, err := service.CreateABTest(req.Item)
	if err == nil {
		return &abtest_grpc.CreateABTestResponse{
			Status: &abtest_grpc.CommonStatus{
				IsOk: true,
				Msg:  "",
			},
		}, nil
	} else {
		return &abtest_grpc.CreateABTestResponse{
			Status: &abtest_grpc.CommonStatus{
				IsOk: false,
				Msg:  err.Error(),
			},
		}, err
	}
}

func (*Handler) UpdateABTest(ctx context.Context, req *abtest_grpc.UpdateABTestRequest) (*abtest_grpc.UpdateABTestResponse, error) {
	abtestItem, err := service.UpdateABTest(req.Item)

	if err != nil {
		return &abtest_grpc.UpdateABTestResponse{
			Status: &abtest_grpc.CommonStatus{
				IsOk: false,
				Msg:  err.Error(),
			},
		}, err
	}
	return &abtest_grpc.UpdateABTestResponse{
		Status: &abtest_grpc.CommonStatus{
			IsOk: true,
			Msg:  "",
		},
		Item: abtestItem,
	}, nil
}

func (*Handler) DeleteABTest(ctx context.Context, req *abtest_grpc.DeleteABTestRequest) (*abtest_grpc.DeleteABTestResponse, error) {
	service.TransABTestStatus(req.AbtestId, abtest.ABTestStatus_DELETED)
	return &abtest_grpc.DeleteABTestResponse{}, nil
}

func (*Handler) GetABTestConfigByPlayer(ctx context.Context, req *abtest_grpc.GetABTestConfigRequest) (*abtest_grpc.GetABTestConfigResponse, error) {
	var err error
	resp, err := service.GetPersonasGrpcClient().Client.GetPersonas(
		ctx,
		&personas_grpc.GetPersonasRequest{
			AppName:  req.App,
			PlayerId: req.PlayerId,
		},
	)

	if err != nil {
		return nil, err
	}

	abtestMap, err := service.GenerateABTestConfigByPersonas(ctx, resp.Personas, req.Extras)
	if err != nil {
		return nil, err
	}

	return &abtest_grpc.GetABTestConfigResponse{AbtestConfig: abtestMap}, nil
}
