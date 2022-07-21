package handlers

import (
	"context"
	"errors"
	"os"
	"sync"

	"github.com/yuhua-zhao/DragonABTest/service"

	"github.com/FlyDragonGO/DragonPlusServerUtils/logging"
	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	abtest_grpc "github.com/FlyDragonGO/ProtobufDefinition/go/grpc/abtest"
	personas_grpc "github.com/FlyDragonGO/ProtobufDefinition/go/grpc/personas"
	"github.com/FlyDragonGO/ProtobufDefinition/go/personas"
)

// handler层
// grpc实现与service层的交互
type Handler struct {
}

var logger = logging.NewLogger("abtest", &logging.LogOptions{
	KinesisStreamName: os.Getenv("KINESIS_STREAM_NAME"),
	KinesisRegion:     os.Getenv("KINESIS_REGION"),
	MaxSize:           16,
	MaxBackups:        3,
})

func (*Handler) GetABTests(ctx context.Context, req *abtest_grpc.GetABTestRequest) (*abtest_grpc.GetABTestResponse, error) {
	logger.WithFields(map[string]interface{}{
		"app_name": req.App,
		"action":   "list_abtests",
		"status":   req.Status,
	}).Info("")

	// 如果没有app 抛错
	if req.App == "" {
		return nil, errors.New("app can not be empty")
	}

	// 默认limit 20
	if req.Limit == 0 {
		req.Limit = 20
	}

	if results, count, err := service.ListABTests(ctx, req.App, req.Status, req.Limit, req.Offset); err == nil {
		return &abtest_grpc.GetABTestResponse{
			Items: results,
			Total: uint32(count),
		}, nil
	} else {
		return nil, err
	}
}

func (*Handler) CreateABTest(ctx context.Context, req *abtest_grpc.CreateABTestRequest) (*abtest_grpc.CreateABTestResponse, error) {
	if createdAbtestItem, err := service.CreateABTest(ctx, req.Item); err == nil {
		return &abtest_grpc.CreateABTestResponse{
			Item: createdAbtestItem,
		}, nil
	} else {
		return nil, err
	}
}

func (*Handler) UpdateABTest(ctx context.Context, req *abtest_grpc.UpdateABTestRequest) (*abtest_grpc.UpdateABTestResponse, error) {
	if updatedAbtestItem, err := service.UpdateABTest(ctx, req.Item); err == nil {
		return &abtest_grpc.UpdateABTestResponse{
			Item: updatedAbtestItem,
		}, nil
	} else {
		return nil, err
	}
}

func (*Handler) DeleteABTest(ctx context.Context, req *abtest_grpc.DeleteABTestRequest) (*abtest_grpc.DeleteABTestResponse, error) {

	if status, err := service.TransABTestStatus(ctx, req.AbtestId, abtest.ABTestStatus_DELETED); err == nil {
		return &abtest_grpc.DeleteABTestResponse{
			Ack: status,
		}, nil
	} else {
		return nil, err
	}
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

	personasItem := resp.Personas

	if err != nil {
		return nil, err
	}

	wg := &sync.WaitGroup{}
	abtestMapChan := make(chan map[string]*personas.PersonaABTestPayload, 1)
	removedKeyChan := make(chan []string, 1)
	defer close(abtestMapChan)
	defer close(removedKeyChan)

	currentKeys := make([]string, 0, len(personasItem.AbtestConfig))
	for configKey := range personasItem.AbtestConfig {
		currentKeys = append(currentKeys, configKey)
	}

	wg.Add(2)
	go service.AsyncGenerateABTestConfigByPersonas(wg, abtestMapChan, personasItem, req.Extras)
	go service.AsyncGetRemovedABTests(wg, removedKeyChan, req.App, currentKeys)

	wg.Wait()
	abtestMap := <-abtestMapChan
	removedKeys := <-removedKeyChan

	for _, k := range removedKeys {
		delete(abtestMap, k)
	}

	if len(abtestMap) > 0 {
		service.GetPersonasGrpcClient().Client.UpdatePersonas(
			context.Background(),
			&personas_grpc.UpdatePersonasRequest{
				Personas: &personas.Personas{
					App:          personasItem.App,
					PlayerId:     personasItem.PlayerId,
					AbtestConfig: abtestMap,
				},
			},
		)
	}

	return &abtest_grpc.GetABTestConfigResponse{AbtestConfig: abtestMap}, nil
}
