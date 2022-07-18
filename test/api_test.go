package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	pb "github.com/FlyDragonGO/ProtobufDefinition/go/grpc/abtest"
	"github.com/FlyDragonGO/ProtobufDefinition/go/personas"
	"github.com/yuhua-zhao/DragonABTest/handlers"
)

func TestCreate(t *testing.T) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")

	nowTs := uint64(time.Now().Unix())
	req := &pb.CreateABTestRequest{
		Item: &abtest.ABTestItem{
			App:          "aaa",
			Name:         "测试测试",
			Desc:         "描述描述",
			TestStart:    nowTs,
			TestEnd:      nowTs + 12345678,
			ParameterKey: "Test_Key",
			ExperimentItems: []*abtest.ExperimentItem{
				{Id: 0, Config: "", Type: abtest.ExperimentType_OBSERVATION, Flow: []uint32{1, 2, 3}},
				{Id: 1, Config: "", Type: abtest.ExperimentType_EXPERIMENT, Flow: []uint32{4, 5, 6}},
			},
			LastEtag: "",
			Status:   abtest.ABTestStatus_DRAFT,
		},
	}
	fmt.Println(req)
	// handler := &handlers.Handler{}
	// resp, err := handler.CreateABTest(context.TODO(), req)
	// if err != nil {
	// 	fmt.Println(err)
	// } else {
	// 	fmt.Println(resp)
	// }
}

func TestUpdate(t *testing.T) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
	nowTs := uint64(time.Now().Unix())
	req := &pb.UpdateABTestRequest{
		Item: &abtest.ABTestItem{
			Id:           "62cd191f13b4bf9f57aa563e",
			App:          "App Field",
			Name:         "Name Field",
			Desc:         "Desc Field",
			TestStart:    nowTs,
			TestEnd:      nowTs + 10000,
			ParameterKey: "Parameter Key Field",
			OrConditions: []*abtest.ABTestOrCondition{
				{
					AndConditions: []*abtest.ABTestAndCondition{
						{
							Filters: []*abtest.ABTestFilter{
								{
									Key:      "country",
									Operator: abtest.FilterOperator_EQUAL,
									StrValue: "US",
								},
								{
									Key:      "player_type",
									Operator: abtest.FilterOperator_EQUAL,
									IntValue: 2,
								},
							},
						},
					},
				},
			},
			ExperimentItems: []*abtest.ExperimentItem{
				{
					Id:     0,
					Type:   abtest.ExperimentType_OBSERVATION,
					Flow:   []uint32{1, 2, 3, 4, 5, 6, 7, 8, 72, 83},
					Config: "aaaaaa",
				},
			},
			LastEtag: "xxxx",
			Status:   abtest.ABTestStatus_PUBLISHED,
		},
	}
	fmt.Println(req)
	// handler := &handlers.Handler{}
	// resp, err := handler.UpdateABTest(context.TODO(), req)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println(resp)
}

func TestList(t *testing.T) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
	req := &pb.GetABTestRequest{
		Status: abtest.ABTestStatus_DRAFT,
		Limit:  20,
		Offset: 0,
	}
	handler := &handlers.Handler{}
	resp, err := handler.GetABTests(context.TODO(), req)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(resp.Items)
	}
}

func TestGetConfigByPersona(t *testing.T) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
	req := &pb.GetABTestConfigRequest{
		Personas: &personas.Personas{
			App:        "App Field",
			PlayerId:   123,
			Country:    "US",
			PlayerType: 2,
		},
	}
	handler := handlers.Handler{}
	resp, _ := handler.GetABTestConfigByPersona(context.TODO(), req)
	fmt.Print(resp)
}

func TestDelete(t *testing.T) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
	req := &pb.DeleteABTestRequest{
		AbtestId: "62cd191f13b4bf9f57aa563e",
	}
	handler := &handlers.Handler{}

	resp, err := handler.DeleteABTest(context.TODO(), req)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(resp)
}
