package test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	pb "github.com/FlyDragonGO/ProtobufDefinition/go/grpc/abtest"
	"github.com/yuhua-zhao/DragonABTest/handlers"
)

// func TestCreate(t *testing.T) {
// 	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")

// 	nowTs := uint64(time.Now().Unix())
// 	req := &pb.CreateABTestRequest{
// 		Item: &abtest.ABTestItem{
// 			App:          "aaa",
// 			Name:         "测试测试",
// 			Desc:         "描述描述",
// 			TestStart:    nowTs,
// 			TestEnd:      nowTs + 12345678,
// 			ParameterKey: "Test_Key",
// 			ExperimentItems: []*abtest.ExperimentItem{
// 				{Id: 0, Config: "", Type: abtest.ExperimentType_OBSERVATION, Flow: []uint32{1, 2, 3}},
// 				{Id: 1, Config: "", Type: abtest.ExperimentType_EXPERIMENT, Flow: []uint32{4, 5, 6}},
// 			},
// 			LastEtag: "",
// 			Status:   abtest.ABTestStatus_DRAFT,
// 		},
// 	}
// 	fmt.Println(req)
// 	// handler := &handlers.Handler{}
// 	// resp, err := handler.CreateABTest(context.TODO(), req)
// 	// if err != nil {
// 	// 	fmt.Println(err)
// 	// } else {
// 	// 	fmt.Println(resp)
// 	// }
// }

func TestUpdate(t *testing.T) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
	// nowTs := uint64(time.Now().Unix())
	req := &pb.UpdateABTestRequest{
		Item: &abtest.ABTestItem{
			Id:           "62cd191f13b4bf9f57aa563e",
			App:          "app",
			Name:         "this is a abtest",
			Desc:         "Desc Field",
			FlowLimit:    100,
			ParameterKey: "abtest_parameter_key",
			AndConditions: []*abtest.ABTestAndCondition{
				{
					Filters: []*abtest.ABTestFilter{
						{
							Key:      "country",
							Operator: abtest.FilterOperator_EQUAL,
							StrValue: "US",
						},
					},
				},
			},
			ExperimentItems: []*abtest.ExperimentItem{
				{
					Id:     0,
					Type:   abtest.ExperimentType_OBSERVATION,
					Config: "aaaaaa",
				},
				{
					Id:     1,
					Type:   abtest.ExperimentType_EXPERIMENT,
					Config: "aaaaaaa",
				},
			},
			LastEtag: "xxxx",
			Status:   abtest.ABTestStatus_PUBLISHED,
		},
	}
	// fmt.Println(req)
	handler := &handlers.Handler{}
	resp, err := handler.UpdateABTest(context.TODO(), req)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(resp)
}

// func TestList(t *testing.T) {
// 	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
// 	req := &pb.GetABTestRequest{
// 		Status: abtest.ABTestStatus_DRAFT,
// 		Limit:  20,
// 		Offset: 0,
// 	}
// 	handler := &handlers.Handler{}
// 	resp, err := handler.GetABTests(context.TODO(), req)
// 	if err != nil {
// 		fmt.Println(err)
// 	} else {
// 		fmt.Println(resp.Items)
// 	}
// }

func BenchmarkGetConfigByPersona(b *testing.B) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
	os.Setenv("PROCESS_ENV", "local")
	handler := handlers.Handler{}
	for i := 0; i < b.N; i++ {
		req := &pb.GetABTestConfigRequest{
			App:      "App",
			PlayerId: uint64(i),
		}
		handler.GetABTestConfigByPlayer(context.TODO(), req)
	}
}

func TestGetConfigByPersona(t *testing.T) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
	os.Setenv("PROCESS_ENV", "local")
	req := &pb.GetABTestConfigRequest{
		App:      "app",
		PlayerId: 1234,
	}
	handler := handlers.Handler{}
	resp, err := handler.GetABTestConfigByPlayer(context.TODO(), req)
	fmt.Println(err)
	fmt.Println(resp)
}

// func TestDelete(t *testing.T) {
// 	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
// 	req := &pb.DeleteABTestRequest{
// 		AbtestId: "62cd191f13b4bf9f57aa563e",
// 	}
// 	handler := &handlers.Handler{}

// 	resp, err := handler.DeleteABTest(context.TODO(), req)
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Println(resp)
// }
