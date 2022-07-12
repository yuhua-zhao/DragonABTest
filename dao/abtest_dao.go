package dao

import (
	"strings"

	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	"github.com/FlyDragonGO/ProtobufDefinition/go/personas"
	"github.com/spaolacci/murmur3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ABTestFilterDao struct {
	Key         string    `bson:"key"`
	Operator    int32     `bson:"operator"`
	StrValue    string    `bson:"str_value"`
	StrValues   []string  `bson:"str_values"`
	IntValue    uint64    `bson:"int_value"`
	IntValues   []uint64  `bson:"int_values"`
	FloatValue  float32   `bson:"float_value"`
	FloatValues []float32 `bson:"float_values"`
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
	Id              primitive.ObjectID      `bson:"_id,omitempty"`
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

func (filterDao *ABTestFilterDao) floatValueCompare(value float32) (bool, error) {
	if filterDao.Operator == int32(abtest.FilterOperator_EQUAL) {
		return filterDao.FloatValue == value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_NOT_EQUAL) {
		return filterDao.FloatValue != value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_GREATER) {
		return filterDao.FloatValue > value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_GREATER_EQUAL) {
		return filterDao.FloatValue >= value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_SMALLER) {
		return filterDao.FloatValue < value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_SMALLER_EQUAL) {
		return filterDao.FloatValue <= value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_IN) {
		for _, x := range filterDao.FloatValues {
			if x == value {
				return true, nil
			}
		}
		return false, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_NOT_IN) {
		for _, x := range filterDao.FloatValues {
			if x == value {
				return false, nil
			}
		}
		return true, nil
	}

	return false, nil
}

func (filterDao *ABTestFilterDao) intValueCompare(value uint64) (bool, error) {

	if filterDao.Operator == int32(abtest.FilterOperator_EQUAL) {
		return filterDao.IntValue == value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_NOT_EQUAL) {
		return filterDao.IntValue != value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_GREATER) {
		return filterDao.IntValue > value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_GREATER_EQUAL) {
		return filterDao.IntValue >= value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_SMALLER) {
		return filterDao.IntValue < value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_SMALLER_EQUAL) {
		return filterDao.IntValue <= value, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_IN) {
		for _, x := range filterDao.IntValues {
			if x == value {
				return true, nil
			}
		}
		return false, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_NOT_IN) {
		for _, x := range filterDao.IntValues {
			if x == value {
				return false, nil
			}
		}
		return true, nil
	}
	return false, nil
}

func (filterDao *ABTestFilterDao) stringValueCompare(value string) (bool, error) {
	if filterDao.Operator == int32(abtest.FilterOperator_EQUAL) {
		return value == filterDao.StrValue, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_NOT_EQUAL) {
		return value == filterDao.StrValue, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_IN) {
		for _, x := range filterDao.StrValues {
			if x == value {
				return true, nil
			}
		}
		return false, nil
	}
	if filterDao.Operator == int32(abtest.FilterOperator_NOT_IN) {
		for _, x := range filterDao.StrValues {
			if x == value {
				return false, nil
			}
		}
		return true, nil
	}
	return false, nil
}

func (filterDao *ABTestFilterDao) PersonasCompare(persona *personas.Personas) bool {

	var flag bool = false
	switch filterDao.Key {
	case "player_type":
		flag, _ = filterDao.intValueCompare(uint64(persona.PlayerType))
	case "installed_at":
		flag, _ = filterDao.intValueCompare(persona.InstalledAt)
	case "first_pay_date":
		flag, _ = filterDao.intValueCompare(persona.FirstPayDate)
	case "last_pay_date":
		flag, _ = filterDao.intValueCompare(persona.LastPayDate)
	case "iap_total":
		flag, _ = filterDao.intValueCompare(persona.IapTotal)
	case "iap_count":
		flag, _ = filterDao.intValueCompare(persona.IapCount)
	case "last_login_date":
		flag, _ = filterDao.intValueCompare(persona.LastLoginDate)
	case "login_time":
		flag, _ = filterDao.intValueCompare(persona.LoginTime)
	case "login_count":
		flag, _ = filterDao.intValueCompare(persona.LoginCount)
	case "last_level_date":
		flag, _ = filterDao.intValueCompare(persona.LastLevelDate)
	case "max_level_id":
		flag, _ = filterDao.intValueCompare(persona.MaxLevelId)
	case "marketing_cpi":
		flag, _ = filterDao.floatValueCompare(persona.MarketingCpi)
	case "revenue_ads_total":
		flag, _ = filterDao.floatValueCompare(persona.RevenueAdsTotal)
	case "country":
		flag, _ = filterDao.stringValueCompare(persona.Country)
	case "platform":
		flag, _ = filterDao.stringValueCompare(persona.Platform)
	case "client_version":
		flag, _ = filterDao.stringValueCompare(persona.ClientVersion)
	case "res_version":
		flag, _ = filterDao.stringValueCompare(persona.ResVersion)
	case "device_id":
		flag, _ = filterDao.stringValueCompare(persona.DeviceId)
	case "device_memory":
		flag, _ = filterDao.stringValueCompare(persona.DeviceMemory)
	case "device_model":
		flag, _ = filterDao.stringValueCompare(persona.DeviceModel)
	case "device_type":
		flag, _ = filterDao.stringValueCompare(persona.DeviceType)
	case "device_os_version":
		flag, _ = filterDao.stringValueCompare(persona.DeviceOsVersion)
	case "device_language":
		flag, _ = filterDao.stringValueCompare(persona.DeviceLanguage)
	case "network_type":
		flag, _ = filterDao.stringValueCompare(persona.NetworkType)
	case "email":
		flag, _ = filterDao.stringValueCompare(persona.Email)
	case "facebook_email":
		flag, _ = filterDao.stringValueCompare(persona.FacebookEmail)
	case "facebook_name":
		flag, _ = filterDao.stringValueCompare(persona.FacebookName)
	case "facebook_id":
		flag, _ = filterDao.stringValueCompare(persona.FacebookId)
	case "source_network":
		flag, _ = filterDao.stringValueCompare(persona.SourceNetwork)
	case "source_campaign":
		flag, _ = filterDao.stringValueCompare(persona.SourceCampaign)
	case "source_adgroup":
		flag, _ = filterDao.stringValueCompare(persona.SourceAdgroup)
	case "source_creative":
		flag, _ = filterDao.stringValueCompare(persona.SourceCreative)
	case "most_pay_product_id":
		flag, _ = filterDao.stringValueCompare(persona.MostPayProductId)
	case "recent_pay_product_id":
		flag, _ = filterDao.stringValueCompare(persona.RecentPayProductId)
	}
	return flag
}

func (filterDao *ABTestFilterDao) TransToProtobuf() *abtest.ABTestFilter {
	return &abtest.ABTestFilter{
		Key:         filterDao.Key,
		Operator:    abtest.FilterOperator(filterDao.Operator),
		StrValue:    filterDao.StrValue,
		StrValues:   filterDao.StrValues,
		IntValue:    filterDao.IntValue,
		IntValues:   filterDao.IntValues,
		FloatValue:  filterDao.FloatValue,
		FloatValues: filterDao.FloatValues,
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

func MapABTestItemArray(abtestItemDaoArray []*ABTestItemDao) []*abtest.ABTestItem {
	abtestItemArray := make([]*abtest.ABTestItem, len(abtestItemDaoArray))
	for i, x := range abtestItemDaoArray {
		abtestItemArray[i] = x.TransToProtobuf()
	}
	return abtestItemArray
}

func NewABTestFilterDao(abtestFilter *abtest.ABTestFilter) *ABTestFilterDao {
	return &ABTestFilterDao{
		Key:         abtestFilter.Key,
		Operator:    int32(abtestFilter.Operator),
		IntValue:    abtestFilter.IntValue,
		IntValues:   abtestFilter.IntValues,
		StrValue:    abtestFilter.StrValue,
		StrValues:   abtestFilter.StrValues,
		FloatValue:  abtestFilter.FloatValue,
		FloatValues: abtestFilter.FloatValues,
	}
}

func NewABTestAndConditionDao(abTestAndConditions *abtest.ABTestAndCondition) *ABTestAndConditionDao {
	filterDaos := []*ABTestFilterDao{}

	for _, filterItem := range abTestAndConditions.Filters {
		filterDaos = append(filterDaos, NewABTestFilterDao(filterItem))
	}

	return &ABTestAndConditionDao{
		Filters: filterDaos,
	}
}

func NewABTestOrConditionDao(abTestOrCondition *abtest.ABTestOrCondition) *ABTestOrConditionDao {
	andConditionDaos := []*ABTestAndConditionDao{}
	for _, andConditionItem := range abTestOrCondition.AndConditions {
		andConditionDaos = append(andConditionDaos, NewABTestAndConditionDao(andConditionItem))
	}
	return &ABTestOrConditionDao{
		AndConditions: andConditionDaos,
	}
}

func NewExperimentItemDao(experimentItem *abtest.ExperimentItem) *ExperimentItemDao {
	return &ExperimentItemDao{
		Id:     experimentItem.Id,
		Config: experimentItem.Config,
		Type:   int32(experimentItem.Type),
		Flow:   experimentItem.Flow,
	}
}

func NewABTestItemDao(abtestItem *abtest.ABTestItem) *ABTestItemDao {
	abtestItemDao := &ABTestItemDao{
		App:          abtestItem.App,
		Name:         abtestItem.Name,
		Desc:         abtestItem.Desc,
		TestStart:    abtestItem.TestStart,
		TestEnd:      abtestItem.TestEnd,
		ParameterKey: abtestItem.ParameterKey,
		LastEtag:     abtestItem.LastEtag,
		Status:       int32(abtestItem.Status),
	}

	if abtestItem.Id != "" && primitive.IsValidObjectID(abtestItem.Id) {
		abtestItemId, err := primitive.ObjectIDFromHex(abtestItem.Id)
		abtestItemDao.Id = abtestItemId
		if err != nil {
			return nil
		}
	}

	if abtestItem.OrConditions != nil {
		orConditionDaos := []*ABTestOrConditionDao{}
		for _, orCondition := range abtestItem.OrConditions {
			orConditionDaos = append(orConditionDaos, NewABTestOrConditionDao(orCondition))
		}
		abtestItemDao.OrConditions = orConditionDaos
	}

	if abtestItem.ExperimentItems != nil {
		experimentItemDaos := []*ExperimentItemDao{}
		for _, experimentItem := range abtestItem.ExperimentItems {
			experimentItemDaos = append(experimentItemDaos, NewExperimentItemDao(experimentItem))
		}
		abtestItemDao.ExperimentItems = experimentItemDaos
	}

	return abtestItemDao
}

func (abtestItemDao *ABTestItemDao) GenerateABTestConfig(app string, playerId uint64) (string, string) {
	keys := []string{app, string(playerId), abtestItemDao.Id.Hex()}

	hashInt := murmur3.Sum32([]byte(strings.Join(keys, "|")))

	personaGroup := hashInt % 100

	for _, experiment_item := range abtestItemDao.ExperimentItems {
		for _, flow := range experiment_item.Flow {
			if flow == personaGroup {
				return abtestItemDao.ParameterKey, string(experiment_item.Id)
			}
		}
	}
	return "", ""
}
