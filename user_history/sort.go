package user_history

import (
	"github.com/customerio/homework/models"
	"sort"
)

func SortAttributes(attributes map[string]*models.Attribute) []string {
	sorted := make([]string, len(attributes))

	i := 0
	for attributeName := range attributes {
		sorted[i] = attributeName
		i++
	}

	sort.Strings(sorted)

	return sorted
}

func SortEvents(events map[string]*models.Event) []string {
	sorted := make([]string, len(events))

	i := 0
	for eventName := range events {
		sorted[i] = eventName
		i++
	}

	sort.Strings(sorted)

	return sorted
}
