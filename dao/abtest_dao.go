package dao

import (
	"fmt"
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

// type ABTestOrConditionDao struct {
// 	AndConditions []*ABTestAndConditionDao `bson:"and_conditions"`
// }

type ExperimentItemDao struct {
	Id     int32    `bson:"id"`
	Config string   `bson:"config"`
	Type   int32    `bson:"type"`
	Flow   []uint32 `bson:"flow"`
}

type ABTestItemDao struct {
	Id              primitive.ObjectID       `bson:"_id,omitempty"`
	App             string                   `bson:"app"`
	Name            string                   `bson:"name"`
	Desc            string                   `bson:"desc"`
	FlowLimit       uint32                   `bson:"flow_limit"`
	ParameterKey    string                   `bson:"parameter_key"`
	AndConditions   []*ABTestAndConditionDao `bson:"and_conditions"`
	ExperimentItems []*ExperimentItemDao     `bson:"experiment_items"`
	LastEtag        string                   `bson:"last_etag"`
	Status          int32                    `bson:"status"`
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
		andCondition.Filters = make([]*abtest.ABTestFilter, len(andConditionDao.Filters))
		for i, v := range andConditionDao.Filters {
			andCondition.Filters[i] = v.TransToProtobuf()
		}
	}
	return andCondition
}

func (experimentItemDao *ExperimentItemDao) TransToProtobuf() *abtest.ExperimentItem {
	return &abtest.ExperimentItem{
		Id:     experimentItemDao.Id,
		Config: experimentItemDao.Config,
		Type:   abtest.ExperimentType(experimentItemDao.Type),
	}
}

func (abtestItemDao *ABTestItemDao) TransToProtobuf() *abtest.ABTestItem {
	var andConditions []*abtest.ABTestAndCondition
	var experimentItems []*abtest.ExperimentItem

	if abtestItemDao.AndConditions != nil {
		andConditions = make([]*abtest.ABTestAndCondition, len(abtestItemDao.AndConditions))
		for i, v := range abtestItemDao.AndConditions {
			andConditions[i] = v.TransToProtobuf()
		}
	}
	if abtestItemDao.ExperimentItems != nil {
		experimentItems = make([]*abtest.ExperimentItem, len(abtestItemDao.ExperimentItems))
		for i, v := range abtestItemDao.ExperimentItems {
			experimentItems[i] = v.TransToProtobuf()
		}
	}
	abtestItem := &abtest.ABTestItem{
		Id:              abtestItemDao.Id.String(),
		App:             abtestItemDao.App,
		Name:            abtestItemDao.Name,
		Desc:            abtestItemDao.Desc,
		FlowLimit:       abtestItemDao.FlowLimit,
		ParameterKey:    abtestItemDao.ParameterKey,
		AndConditions:   andConditions,
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
	var filterDaos []*ABTestFilterDao
	if abTestAndConditions.Filters != nil && len(abTestAndConditions.Filters) > 0 {
		filterDaos = make([]*ABTestFilterDao, len(abTestAndConditions.Filters))
		for i, filterItem := range abTestAndConditions.Filters {
			filterDaos[i] = NewABTestFilterDao(filterItem)
		}
	}

	return &ABTestAndConditionDao{
		Filters: filterDaos,
	}
}

func NewExperimentItemDao(experimentItem *abtest.ExperimentItem) *ExperimentItemDao {
	return &ExperimentItemDao{
		Id:     experimentItem.Id,
		Config: experimentItem.Config,
		Type:   int32(experimentItem.Type),
	}
}

func NewABTestItemDao(abtestItem *abtest.ABTestItem) *ABTestItemDao {
	abtestItemDao := &ABTestItemDao{
		App:          abtestItem.App,
		Name:         abtestItem.Name,
		Desc:         abtestItem.Desc,
		FlowLimit:    abtestItem.FlowLimit,
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

	if abtestItem.AndConditions != nil && len(abtestItem.AndConditions) > 0 {
		andConditionDaos := make([]*ABTestAndConditionDao, len(abtestItem.AndConditions))
		for i, andCondition := range abtestItem.AndConditions {
			andConditionDaos[i] = NewABTestAndConditionDao(andCondition)
		}
		abtestItemDao.AndConditions = andConditionDaos
	}

	if abtestItem.ExperimentItems != nil && len(abtestItem.ExperimentItems) > 0 {
		experimentItemDaos := make([]*ExperimentItemDao, len(abtestItem.ExperimentItems))
		for i, experimentItem := range abtestItem.ExperimentItems {
			experimentItemDaos[i] = NewExperimentItemDao(experimentItem)
		}
		abtestItemDao.ExperimentItems = experimentItemDaos
	}

	return abtestItemDao
}

func (abtestItemDao *ABTestItemDao) EnsurePersonasFit(personas *personas.Personas) bool {
	var currentABTestFit = false
	for _, andCondition := range abtestItemDao.AndConditions {
		currentAndConditionFlag := true
		for _, filterItem := range andCondition.Filters {
			currentAndConditionFlag = currentAndConditionFlag && filterItem.PersonasCompare(personas)
			if !currentAndConditionFlag {
				break
			}
		}
		currentABTestFit = currentABTestFit || currentAndConditionFlag
		if currentABTestFit {
			break
		}
	}
	return currentABTestFit
}

func (abtestItemDao *ABTestItemDao) EnsureABTestExperimentItemByFlow(flow uint32) uint32 {
	experimentItemsLen := len(abtestItemDao.ExperimentItems)
	return flow % uint32(experimentItemsLen)
}

func (abtestItemDao *ABTestItemDao) CalculatePersonasHash(personas *personas.Personas) uint32 {
	keys := []string{personas.App, fmt.Sprint(personas.PlayerId), abtestItemDao.Id.Hex()}
	return murmur3.Sum32([]byte(strings.Join(keys, "|"))) % 1000
}
