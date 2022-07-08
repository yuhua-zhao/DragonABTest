package service

import (
	"context"
	"fmt"

	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	"github.com/yuhua-zhao/DragonABTest/dao"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ListABTests(app string, limit int, offset int, status abtest.ABTestStatus) ([]*abtest.ABTestItem, int64, error) {
	filter := bson.M{}
	if app != "" {
		filter["app"] = app
	}
	if status != abtest.ABTestStatus_UNKNOW {
		filter["status"] = status
	}
	limit64, offset64 := int64(limit), int64(offset)
	count, err := dao.GetInstance().ABTest.CountDocuments(context.TODO(), filter)
	if err != nil {
		return nil, 0, err
	}
	cursor, err := dao.GetInstance().ABTest.Find(
		context.TODO(),
		filter,
		&options.FindOptions{
			Sort:  map[string]int{"test_end": -1},
			Skip:  &offset64,
			Limit: &limit64,
		},
	)
	if err != nil {
		return nil, 0, err
	}
	var result []*abtest.ABTestItem
	cursor.All(context.TODO(), &result)
	return result, count, nil
}

func UpsertABTest(abtestItem *abtest.ABTestItem) (*abtest.ABTestItem, error) {
	if abtestItem.Id == "" {
		insertResult, err := dao.GetInstance().ABTest.InsertOne(context.TODO(), abtestItem)
		if err != nil {
			return nil, err
		}
		fmt.Println(insertResult.InsertedID)
		// abtestItem.Id =
	} else {
		docId, err := primitive.ObjectIDFromHex(abtestItem.Id)
		if err != nil {
			return nil, err
		}
		dao.GetInstance().ABTest.FindOneAndReplace(
			context.TODO(),
			bson.M{
				"_id": docId,
				"app": abtestItem.App,
			},
			abtestItem,
		)
	}
	return abtestItem, nil
	// var result = dao.GetInstance().ABTest.FindOneAndReplace(
	// 	context.TODO(),
	// 	bson.M{
	// 		"app": abtestItem.App,
	// 		"id":  abtestItem.Id,
	// 	},
	// 	abtestItem,
	// 	&options.FindOneAndReplaceOptions{
	// 		Upsert: &upsertFlag,
	// 	},
	// )
	// decodeResult := &abtest.ABTestItem{}
	// if err := result.Decode(decodeResult); err != nil {
	// 	return nil, err
	// }
	// return decodeResult, nil
}
