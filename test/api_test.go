package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	pb "github.com/FlyDragonGO/ProtobufDefinition/go/grpc/abtest"
	"github.com/yuhua-zhao/DragonABTest/handlers"
)

func TestUpsertApi(t *testing.T) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
	os.Setenv("mongo_db", "ABTest")

	handler := handlers.Handler{}
	nowTs := uint64(time.Now().Unix())
	req := &pb.CreateABTestRequest{Item: &abtest.ABTestItem{
		// Id:           "123",
		App:          "aaa",
		Name:         "aaa_test",
		Desc:         "aaa_desc",
		TestStart:    nowTs,
		TestEnd:      nowTs + 1234567,
		ParameterKey: "a_parameter",
		OrConditions: nil,
		ExperimentItems: []*abtest.ExperimentItem{
			{
				Id:     0,
				Config: "{'aaa': 'bbb'}",
				Type:   abtest.ExperimentType_OBSERVATION,
				Flow:   []uint32{1, 2, 3, 4, 5},
			},
			{
				Id:     1,
				Config: "{'aaa': 'bbb'}",
				Type:   abtest.ExperimentType_EXPERIMENT,
				Flow:   []uint32{6, 7, 8, 9, 10},
			},
		},
		Status: abtest.ABTestStatus_DRAFT,
	}}
	resp, err := handler.CreateABTest(context.TODO(), req)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Print(resp.Item)
	}
}

func TestGetApi(t *testing.T) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
	os.Setenv("mongo_db", "ABTest")
	handler := handlers.Handler{}
	req := &pb.GetABTestRequest{
		//App: "aaa",
		//Status: abtest.ABTestStatus_PUBLISHED,
		Limit:  20,
		Offset: 0,
	}
	resp, err := handler.GetABTests(context.TODO(), req)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(resp.Items)
	}
}
