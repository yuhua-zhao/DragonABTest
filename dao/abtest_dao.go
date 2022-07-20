// @Description  服务于protobuf <-> dao <-> bson 之间的互相转换

package dao

import (
	"github.com/FlyDragonGO/ProtobufDefinition/go/abtest"
	"github.com/FlyDragonGO/ProtobufDefinition/go/personas"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// 过滤条件的dao模型
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

// 合并的且逻辑的dao模型
type ABTestAndConditionDao struct {
	Filters []*ABTestFilterDao `bson:"filters"`
}

// 实验(观察)组的dao模型
type ExperimentItemDao struct {
	Id     int32    `bson:"id"`
	Config string   `bson:"config"`
	Type   int32    `bson:"type"`
	Flow   []uint32 `bson:"flow"`
}

// ab测实体的dao模型
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

// 基于filter dao模型做浮点类型数据比较
func (filterDao *ABTestFilterDao) floatValueCompare(value float32) (bool, error) {
	// ==
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_EQUAL {
		return filterDao.FloatValue == value, nil
	}

	// !=
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_NOT_EQUAL {
		return filterDao.FloatValue != value, nil
	}

	// >
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_GREATER {
		return filterDao.FloatValue > value, nil
	}

	// >=
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_GREATER_EQUAL {
		return filterDao.FloatValue >= value, nil
	}

	// <
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_SMALLER {
		return filterDao.FloatValue < value, nil
	}

	// <=
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_SMALLER_EQUAL {
		return filterDao.FloatValue <= value, nil
	}

	// value in xxxx
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_IN {
		for _, x := range filterDao.FloatValues {
			if x == value {
				return true, nil
			}
		}
		return false, nil
	}

	// value not in xxxx
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_NOT_IN {
		for _, x := range filterDao.FloatValues {
			if x == value {
				return false, nil
			}
		}
		return true, nil
	}

	return false, nil
}

// 基于filter dao模型做整型类型数据比较
func (filterDao *ABTestFilterDao) intValueCompare(value uint64) (bool, error) {
	// ==
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_EQUAL {
		return filterDao.IntValue == value, nil
	}

	// !=
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_NOT_EQUAL {
		return filterDao.IntValue != value, nil
	}

	// >
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_GREATER {
		return filterDao.IntValue > value, nil
	}

	// >=
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_GREATER_EQUAL {
		return filterDao.IntValue >= value, nil
	}

	// <
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_SMALLER {
		return filterDao.IntValue < value, nil
	}

	// <=
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_SMALLER_EQUAL {
		return filterDao.IntValue <= value, nil
	}

	// value in xxx
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_IN {
		for _, x := range filterDao.IntValues {
			if x == value {
				return true, nil
			}
		}
		return false, nil
	}

	// value not in xxx
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_NOT_IN {
		for _, x := range filterDao.IntValues {
			if x == value {
				return false, nil
			}
		}
		return true, nil
	}

	return false, nil
}

// 基于filter dao模型做字符类型数据比较
func (filterDao *ABTestFilterDao) stringValueCompare(value string) (bool, error) {
	// ==
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_EQUAL {
		return value == filterDao.StrValue, nil
	}

	// !=
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_NOT_EQUAL {
		return value != filterDao.StrValue, nil
	}

	// value in [xxx]
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_IN {
		for _, x := range filterDao.StrValues {
			if x == value {
				return true, nil
			}
		}
		return false, nil
	}

	// value not in [xxx]
	if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_NOT_IN {
		for _, x := range filterDao.StrValues {
			if x == value {
				return false, nil
			}
		}
		return true, nil
	}
	return false, nil
}

// 基于filter dao模型进行personas是否满足条件的比较
func (filterDao *ABTestFilterDao) personasCompare(persona *personas.Personas) bool {
	// 默认不满足
	var flag bool = false
	// 根据key决定比较的数据类型
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

// 基于filter dao模型进行客户端的额外字段比较
func (filterDao *ABTestFilterDao) filterCompare(clientFilter map[string]string) bool {
	// 先确认字段在filter中存在
	if clientValue, founded := clientFilter[filterDao.Key]; founded {
		// ==
		if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_EQUAL {
			return clientValue == filterDao.StrValue
		}

		// !=
		if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_NOT_EQUAL {
			return clientValue != filterDao.StrValue
		}

		// value in [xxx]
		if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_IN {
			for _, strValue := range filterDao.StrValues {
				if strValue == clientValue {
					return true
				}
			}
			return false
		}

		// value not in [xxx]
		if abtest.FilterOperator(filterDao.Operator) == abtest.FilterOperator_EQUAL {
			for _, strValue := range filterDao.StrValues {
				if strValue == clientValue {
					return false
				}
			}
			return true
		}

		return false
	} else {
		// 如果不存在默认为
		return true
	}
}

// filter的dao -> proto转换
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

// andCondition的dao -> proto转换
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

// experimentItem的dao -> proto转换
func (experimentItemDao *ExperimentItemDao) TransToProtobuf() *abtest.ExperimentItem {
	return &abtest.ExperimentItem{
		Id:     experimentItemDao.Id,
		Config: experimentItemDao.Config,
		Type:   abtest.ExperimentType(experimentItemDao.Type),
	}
}

// abtestItem的dao -> proto转换
func (abtestItemDao *ABTestItemDao) TransToProtobuf() *abtest.ABTestItem {
	abtestItem := &abtest.ABTestItem{
		Id:           abtestItemDao.Id.String(),
		App:          abtestItemDao.App,
		Name:         abtestItemDao.Name,
		Desc:         abtestItemDao.Desc,
		FlowLimit:    abtestItemDao.FlowLimit,
		ParameterKey: abtestItemDao.ParameterKey,
		LastEtag:     abtestItemDao.LastEtag,
		Status:       abtest.ABTestStatus(abtestItemDao.Status),
	}

	if abtestItemDao.AndConditions != nil {
		andConditions := make([]*abtest.ABTestAndCondition, len(abtestItemDao.AndConditions))
		for i, v := range abtestItemDao.AndConditions {
			andConditions[i] = v.TransToProtobuf()
		}
		abtestItem.AndConditions = andConditions
	}

	if abtestItemDao.ExperimentItems != nil {
		experimentItems := make([]*abtest.ExperimentItem, len(abtestItemDao.ExperimentItems))
		for i, v := range abtestItemDao.ExperimentItems {
			experimentItems[i] = v.TransToProtobuf()
		}
		abtestItem.ExperimentItems = experimentItems
	}

	abtestItem.Id = abtestItemDao.Id.String()
	return abtestItem
}

// 将bson读出的list结果映射为proto list
func MapABTestItemArray(abtestItemDaoArray []*ABTestItemDao) []*abtest.ABTestItem {
	abtestItemArray := make([]*abtest.ABTestItem, len(abtestItemDaoArray))
	for i, x := range abtestItemDaoArray {
		abtestItemArray[i] = x.TransToProtobuf()
	}
	return abtestItemArray
}

// 从filter proto映射到dao
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

// 从andCondition proto映射到dao
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

// 从experimentItem proto映射到dao
func NewExperimentItemDao(experimentItem *abtest.ExperimentItem) *ExperimentItemDao {
	return &ExperimentItemDao{
		Id:     experimentItem.Id,
		Config: experimentItem.Config,
		Type:   int32(experimentItem.Type),
	}
}

// 从abtestItem proto映射到dao
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

// 确认personas和客户端的自定义filter符合当前ab测过滤条件
func (abtestItemDao *ABTestItemDao) EnsurePersonasFit(personas *personas.Personas, filter map[string]string) bool {
	var currentABTestFit = false
	for _, andCondition := range abtestItemDao.AndConditions {
		currentAndConditionFlag := true
		for _, filterItem := range andCondition.Filters {
			currentAndConditionFlag = currentAndConditionFlag && filterItem.personasCompare(personas)
			if !currentAndConditionFlag {
				break
			}
			currentAndConditionFlag = currentAndConditionFlag && filterItem.filterCompare(filter)
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
