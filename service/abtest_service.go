package service

import (
	"context"
	"time"
	"unsafe"

	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	"github.com/FlyDragonGO/ProtobufDefinition/go/personas"
	"github.com/yuhua-zhao/DragonABTest/dao"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 列出ABTest
func ListABTests(
	app string,
	status abtest.ABTestStatus,
	limit int,
	offset int,
) ([]*abtest.ABTestItem, int64, error) {
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
	var daoResult []*dao.ABTestItemDao
	cursor.All(context.TODO(), &daoResult)
	respResult := dao.MapABTestItemArray(daoResult)
	return respResult, count, nil
}

// 创建ABTest
func CreateABTest(abtestItem *abtest.ABTestItem) (*abtest.ABTestItem, error) {
	abTestItemDao := dao.NewABTestItemDao(abtestItem)
	insertResult, err := dao.GetInstance().ABTest.InsertOne(context.TODO(), abTestItemDao)
	if err != nil {
		return nil, err
	}
	insertResultId := *(*primitive.ObjectID)(unsafe.Pointer(&insertResult.InsertedID))
	abtestItem.Id = insertResultId.String()
	return abtestItem, nil
}

// 更新ABTest
func UpdateABTest(abtestItem *abtest.ABTestItem) (*abtest.ABTestItem, error) {
	abTestItemDao := dao.NewABTestItemDao(abtestItem)
	var err error
	abTestObjectId, err := primitive.ObjectIDFromHex(abtestItem.Id)
	if err != nil {
		return nil, err
	}

	_, err = dao.GetInstance().ABTest.ReplaceOne(
		context.TODO(),
		bson.M{"_id": abTestObjectId},
		abTestItemDao,
	)

	if err != nil {
		return nil, err
	}

	return abtestItem, nil
}

// 删除ab测试
func TransABTestStatus(abtestId string, abtestStatus abtest.ABTestStatus) (bool, error) {
	abtestObjectId, err := primitive.ObjectIDFromHex(abtestId)
	if err != nil {
		return false, err
	}
	dao.GetInstance().ABTest.UpdateOne(
		context.TODO(),
		bson.M{
			"_id": abtestObjectId,
		},
		bson.M{
			"$set": bson.M{
				"status": abtest.ABTestStatus_DELETED,
			},
		},
	)
	return true, nil
}

// func GetRemovedABTests(ctx context.Context, parameterKeys []string, app string) []string {
// 	cursor, err := dao.GetInstance().ABTest.Find(
// 		ctx,
// 		bson.M{
// 			"app": app,
// 			"parameter_key": bson.M{
// 				"$in": bson.A{parameterKeys},
// 			},
// 			"status": abtest.ABTestStatus_STOPPED,
// 		},
// 		&options.FindOptions{
// 			Projection: bson.M{
// 				"parameter_key": true,
// 			},
// 		},
// 	)
// 	if err != nil {
// 		return []string{}
// 	}
// 	var stoppedParameterKeyList []string
// 	cursor.All(ctx, &stoppedParameterKeyList)
// 	return stoppedParameterKeyList
// }

func GenerateABTestConfigByPersonas(ctx context.Context, persona *personas.Personas, filter interface{}) (map[string]*personas.PersonaABTestPayload, error) {
	var err error
	var daoResult []*dao.ABTestItemDao

	nowTs := uint64(time.Now().Unix())

	// 生成 etags 列表
	// etags := make([]string, len(persona.AbtestConfig))
	// for _, v := range persona.AbtestConfig {
	// 	etags = append(etags, v.LastEtag)
	// }

	// 生成mongo过滤条件
	mongoFilter := bson.M{
		"app":        persona.App,
		"status":     abtest.ABTestStatus_PUBLISHED,
		"test_end":   bson.M{"$gte": nowTs},
		"test_start": bson.M{"$lte": nowTs},
	}
	// if len(etags) > 0 {
	// 	mongoFilter["last_etag"] = bson.M{
	// 		"$nin": bson.A{etags},
	// 	}
	// }

	// 遍历cursor
	var limit int64 = 20
	cursor, err := dao.GetInstance().ABTest.Find(
		ctx,
		mongoFilter,
		&options.FindOptions{
			Sort:  bson.M{"test_end": -1},
			Limit: &limit,
		},
	)
	if err != nil {
		return nil, err
	}
	cursor.All(context.TODO(), &daoResult)

	abtestMap := make(map[string]*personas.PersonaABTestPayload, limit)

	// 遍历筛选出的ab测
	for _, daoItem := range daoResult {
		if value, founded := persona.AbtestConfig[daoItem.ParameterKey]; founded {
			value.GroupId
		}
		var currentABTestFit = false
		// 遍历ab测的过滤条件
		for _, orCondition := range daoItem.OrConditions {
			// 记录当前ab测的 "或" 条件
			var currentOrConditionFlag = false
			// 遍历该ab测下的 "或" 条件下的 "且" 条件
			for _, andCondition := range orCondition.AndConditions {
				// 记录当前的 "且" 条件
				var currentAndConditionFlag = true
				// 遍历该ab测下的 "或" 条件下的 "且" 条件的 过滤标准
				for _, filterItem := range andCondition.Filters {
					// 更新当前且条件的结果, 且条件不满足 可以跳了
					currentAndConditionFlag = currentAndConditionFlag && filterItem.PersonasCompare(persona)
					if !currentAndConditionFlag {
						break
					}
				}
				// 根据且条件更新或条件, 如果已经满足就可以跳了
				currentOrConditionFlag = currentOrConditionFlag || currentAndConditionFlag
				if currentOrConditionFlag {
					break
				}
			}
			// 更新当前ab测的满足条件 如果满足 就跳
			currentABTestFit = currentABTestFit || currentOrConditionFlag
			if currentABTestFit {
				break
			}
		}

		// 当前ab测满足， 生成ab测配置
		if currentABTestFit {
			if userFlow, groupId, lastEtag, parameterKey := daoItem.GenerateABTestConfig(persona); parameterKey != "" {
				abtestMap[parameterKey] = &personas.PersonaABTestPayload{
					GroupId:  groupId,
					UserFlow: userFlow,
					LastEtag: lastEtag,
				}
			}
		}
	}
	return abtestMap, nil
}
