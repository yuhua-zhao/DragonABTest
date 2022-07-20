package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	"github.com/FlyDragonGO/ProtobufDefinition/go/personas"
	"github.com/spaolacci/murmur3"
	"github.com/yuhua-zhao/DragonABTest/dao"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 列出ABTest
func ListABTests(
	ctx context.Context,
	app string,
	status abtest.ABTestStatus,
	limit uint32,
	offset uint32,
) ([]*abtest.ABTestItem, int64, error) {
	filter := bson.M{
		"app": app,
	}

	if status != abtest.ABTestStatus_UNKNOW {
		filter["status"] = status
	}

	limit64, offset64 := int64(limit), int64(offset)

	count, err := dao.GetInstance().ABTest.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	cursor, err := dao.GetInstance().ABTest.Find(
		ctx,
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
	cursor.All(ctx, &daoResult)

	return dao.MapABTestItemArray(daoResult), count, nil
}

// 创建ABTest
func CreateABTest(ctx context.Context, abtestItem *abtest.ABTestItem) (*abtest.ABTestItem, error) {

	abTestItemDao := dao.NewABTestItemDao(abtestItem)
	insertResult, err := dao.GetInstance().ABTest.InsertOne(ctx, abTestItemDao)
	if err != nil {
		return nil, err
	}
	insertResultId := *(*primitive.ObjectID)(unsafe.Pointer(&insertResult.InsertedID))
	abtestItem.Id = insertResultId.String()
	return abtestItem, nil
}

// 更新ABTest
func UpdateABTest(ctx context.Context, abtestItem *abtest.ABTestItem) (*abtest.ABTestItem, error) {
	abTestItemDao := dao.NewABTestItemDao(abtestItem)
	var err error
	abTestObjectId, err := primitive.ObjectIDFromHex(abtestItem.Id)
	if err != nil {
		return nil, err
	}

	_, err = dao.GetInstance().ABTest.ReplaceOne(
		ctx,
		bson.M{"_id": abTestObjectId},
		abTestItemDao,
	)

	if err != nil {
		return nil, err
	}

	return abtestItem, nil
}

// 删除ab测试
func TransABTestStatus(ctx context.Context, abtestId string, abtestStatus abtest.ABTestStatus) (bool, error) {
	abtestObjectId, err := primitive.ObjectIDFromHex(abtestId)
	if err != nil {
		return false, err
	}
	dao.GetInstance().ABTest.UpdateOne(
		ctx,
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

func AsyncGetRemovedABTests(
	wg *sync.WaitGroup,
	resultChan chan []string,
	app string,
	parameterKeys []string,
) {
	defer wg.Done()
	removedKeys := GetRemovedABTests(context.TODO(), parameterKeys, app)
	resultChan <- removedKeys
}

func GetRemovedABTests(ctx context.Context, parameterKeys []string, app string) []string {
	cursor, err := dao.GetInstance().ABTest.Find(
		ctx,
		bson.M{
			"app": app,
			"parameter_key": bson.M{
				"$in": bson.A{parameterKeys},
			},
			"status": abtest.ABTestStatus_STOPPED,
		},
		&options.FindOptions{
			Projection: bson.M{
				"parameter_key": true,
			},
		},
	)
	if err != nil {
		return []string{}
	}
	var stoppedParameterKeyList []string
	cursor.All(ctx, &stoppedParameterKeyList)
	return stoppedParameterKeyList
}

func AsyncGenerateABTestConfigByPersonas(
	wg *sync.WaitGroup,
	resultChan chan map[string]*personas.PersonaABTestPayload,
	personasItem *personas.Personas,
	filter map[string]string,
) {
	defer wg.Done()
	if abtestMap, err := GenerateABTestConfigByPersonas(context.TODO(), personasItem, filter); err == nil {
		resultChan <- abtestMap
	}
}

func GenerateABTestConfigByPersonas(ctx context.Context, personasItem *personas.Personas, filter map[string]string) (map[string]*personas.PersonaABTestPayload, error) {
	var err error
	var daoResult []*dao.ABTestItemDao

	// 生成 etags 列表
	etags := make([]string, len(personasItem.AbtestConfig))
	for _, v := range personasItem.AbtestConfig {
		etags = append(etags, v.LastEtag)
	}

	// 生成mongo过滤条件
	mongoFilter := bson.M{
		"app":    personasItem.App,
		"status": abtest.ABTestStatus_PUBLISHED,
	}
	if len(etags) > 0 {
		mongoFilter["last_etag"] = bson.M{
			"$nin": bson.A{etags},
		}
	}

	// 遍历cursor
	var limit int64 = 20
	cursor, err := dao.GetInstance().ABTest.Find(
		ctx,
		mongoFilter,
		&options.FindOptions{
			Sort:  bson.M{"_id": -1},
			Limit: &limit,
		},
	)
	if err != nil {
		return nil, err
	}
	cursor.All(ctx, &daoResult)
	abtestMap := make(map[string]*personas.PersonaABTestPayload, limit)

	// 遍历筛选出的ab测
	for _, daoItem := range daoResult {

		// 拉已有的数据
		parameterKey := daoItem.ParameterKey
		abtestConfig, founded := personasItem.AbtestConfig[parameterKey]
		experimentCount := int64(len(daoItem.ExperimentItems))

		// 检测到存在配置，且分组数据需要移除(整体流量被缩)
		if founded && abtestConfig.GroupId != -1 && abtestConfig.UserHash > daoItem.FlowLimit {
			abtestMap[parameterKey] = &personas.PersonaABTestPayload{
				GroupId:  -1,
				UserHash: abtestConfig.UserHash,
				LastEtag: daoItem.LastEtag,
			}
		}

		// 检查到存在配置，且需要更新分组数据
		if founded && abtestConfig.GroupId == -1 && abtestConfig.UserHash <= daoItem.FlowLimit {
			abtestMap[parameterKey] = &personas.PersonaABTestPayload{
				GroupId:  int64(abtestConfig.UserHash) % experimentCount,
				UserHash: abtestConfig.UserHash,
				LastEtag: daoItem.LastEtag,
			}
		}

		// 数据不存在，需要生成和计算hash
		if !founded && daoItem.EnsurePersonasFit(personasItem, filter) {
			keys := []string{personasItem.App, fmt.Sprint(personasItem.PlayerId), daoItem.Id.Hex()}
			userHash := murmur3.Sum32([]byte(strings.Join(keys, "|"))) % 1000
			groupId := int64(userHash) % experimentCount
			abtestMap[parameterKey] = &personas.PersonaABTestPayload{
				GroupId:  groupId,
				UserHash: userHash,
				LastEtag: daoItem.LastEtag,
			}
		}
	}
	return abtestMap, nil
}
