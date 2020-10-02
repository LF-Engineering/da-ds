package dads

import "time"

// EmptyAffsItem - return empty affiliation sitem for a given role
func EmptyAffsItem(role string) map[string]interface{} {
	return map[string]interface{}{
		role + "_id":         "",
		role + "_uuid":       "",
		role + "_name":       "",
		role + "_user_name":  "",
		role + "_domain":     "",
		role + "_gender":     "",
		role + "_gender_acc": nil,
		role + "_org_name":   "",
		role + "_bot":        false,
		role + MultiOrgNames: []interface{}{},
	}
}

// IdenityAffsData - add affiliations related data
func IdenityAffsData(identity map[string]interface{}, dt time.Time, role string) (outItem map[string]interface{}) {
	// FIXME: possibly needs to add AffID support
	outItem = EmptyAffsItem(role)
	return
}
