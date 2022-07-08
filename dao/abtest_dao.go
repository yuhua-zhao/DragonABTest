package dao

import (
	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ABTestFilterDao struct {
	Key      string `bson:"key"`
	Operator int32  `bson:"operator"`
	Value    string `bson:"value"`
}

type ABTestAndConditionDao struct {
	Filters []*ABTestFilterDao `bson:"filters"`
}

type ABTestOrConditionDao struct {
	AndConditions []*ABTestAndConditionDao `bson:"and_conditions"`
}

type ExperimentItemDao struct {
	Id     uint32   `bson:"id"`
	Config string   `bson:"config"`
	Type   int32    `bson:"type"`
	Flow   []uint32 `bson:"flow"`
}

type ABTestItemDao struct {
	Id              primitive.ObjectID      `bson:"_id"`
	App             string                  `bson:"app"`
	Name            string                  `bson:"name"`
	Desc            string                  `bson:"desc"`
	TestStart       uint64                  `bson:"test_start"`
	TestEnd         uint64                  `bson:"test_end"`
	ParameterKey    string                  `bson:"parameter_key"`
	OrConditions    []*ABTestOrConditionDao `bson:"or_conditions"`
	ExperimentItems []*ExperimentItemDao    `bson:"experiment_items"`
	LastEtag        string                  `bson:"last_etag"`
	Status          int32                   `bson:"status"`
}

func (filterDao *ABTestFilterDao) TransToProtobuf() *abtest.ABTestFilter {
	return &abtest.ABTestFilter{
		Key:      filterDao.Key,
		Operator: abtest.FilterOperator(filterDao.Operator),
		Value:    filterDao.Value,
	}
}

func (andConditionDao *ABTestAndConditionDao) TransToProtobuf() *abtest.ABTestAndCondition {
	andCondition := &abtest.ABTestAndCondition{}
	if andConditionDao.Filters != nil {
		for _, v := range andConditionDao.Filters {
			andCondition.Filters = append(andCondition.Filters, v.TransToProtobuf())
		}
	}
	return andCondition
}

func (orConditionDao *ABTestOrConditionDao) TransToProtobuf() *abtest.ABTestOrCondition {
	orCondition := &abtest.ABTestOrCondition{}
	if orConditionDao.AndConditions != nil {
		for _, v := range orConditionDao.AndConditions {
			orCondition.AndConditions = append(orCondition.AndConditions, v.TransToProtobuf())
		}
	}
	return orCondition
}

func (experimentItemDao *ExperimentItemDao) TransToProtobuf() *abtest.ExperimentItem {
	return &abtest.ExperimentItem{
		Id:     experimentItemDao.Id,
		Config: experimentItemDao.Config,
		Type:   abtest.ExperimentType(experimentItemDao.Type),
		Flow:   experimentItemDao.Flow,
	}
}

func (abtestItemDao *ABTestItemDao) TransToProtobuf() *abtest.ABTestItem {
	var orConditions []*abtest.ABTestOrCondition
	var experimentItems []*abtest.ExperimentItem

	if abtestItemDao.OrConditions != nil {
		for _, v := range abtestItemDao.OrConditions {
			orConditions = append(orConditions, v.TransToProtobuf())
		}
	}

	if abtestItemDao.ExperimentItems != nil {
		for _, v := range abtestItemDao.ExperimentItems {
			experimentItems = append(experimentItems, v.TransToProtobuf())
		}
	}

	abtestItem := &abtest.ABTestItem{
		Id:              abtestItemDao.Id.String(),
		App:             abtestItemDao.App,
		Name:            abtestItemDao.Name,
		Desc:            abtestItemDao.Desc,
		TestStart:       abtestItemDao.TestStart,
		TestEnd:         abtestItemDao.TestEnd,
		ParameterKey:    abtestItemDao.ParameterKey,
		OrConditions:    orConditions,
		ExperimentItems: experimentItems,
		LastEtag:        abtestItemDao.LastEtag,
		Status:          abtest.ABTestStatus(abtestItemDao.Status),
	}
	abtestItem.Id = abtestItemDao.Id.String()

	return abtestItem
}

//func FromABTest

func FromABTestItem(abtestItem *abtest.ABTestItem) *ABTestItemDao {
	var err error
	abtestItemDao := &ABTestItemDao{
		App:  abtestItem.App,
		Name: abtestItem.Name,
	}

	if abtestItem.Id != "" && primitive.IsValidObjectID(abtestItem.Id) {
		abtestItemDao.Id, err = primitive.ObjectIDFromHex(abtestItem.Id)
		if err != nil {
			return nil
		}
	}

	return abtestItemDao
}
