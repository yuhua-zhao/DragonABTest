package test

import (
	"fmt"
	"os"
	"testing"
	"time"

	abtest "github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	"github.com/yuhua-zhao/DragonABTest/dao"
	"github.com/yuhua-zhao/DragonABTest/service"
)

func TestQueryDao(*testing.T) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017")
	os.Setenv("mongo_db", "ABTest")
	result, count, err := service.ListABTests("test", 20, 0, abtest.ABTestStatus_UNKNOW)
	if err != nil {
		fmt.Println(result)
		fmt.Println(count)
	}
}

func TestInsertDao(t *testing.T) {
	os.Setenv("mongo_uri", "mongodb://mongo:password@127.0.0.1:27017/ABTest")
	os.Setenv("mongo_db", "ABTest")
	nowTs := uint64(time.Now().Unix())
	abtestItem := abtest.ABTestItem{
		App:          "tehansoehu",
		Name:         "nahxcgrcpgf",
		Desc:         "aceoughcaoheu",
		TestStart:    nowTs,
		TestEnd:      nowTs + uint64(10000),
		ParameterKey: "test_parameter",
		OrConditions: []*abtest.ABTestOrCondition{
			{
				AndConditions: []*abtest.ABTestAndCondition{
					{
						Filters: []*abtest.ABTestFilter{
							{
								Key:      "what",
								Operator: abtest.FilterOperator_EQUAL,
								Value:    "this",
							},
						},
					},
				},
			},
		},
		ExperimentItems: []*abtest.ExperimentItem{
			{
				Id:   123,
				Type: abtest.ExperimentType_EXPERIMENT,
				Flow: []uint32{1, 2, 3, 4},
			},
			{
				Id:   456,
				Type: abtest.ExperimentType_OBSERVATION,
				Flow: []uint32{1, 2, 3, 4},
			},
		},
	}
	dao.GetInstance().InsertABTest(abtestItem)
}
