package user_history

import (
	"github.com/customerio/homework/models"
	"sort"
)

func SortUserIds(users map[int]*models.User) []int {
	sorted := make([]int, len(users))

	i := 0
	for userId := range users {
		sorted[i] = userId
		i++
	}

	sort.Ints(sorted)

	return sorted
}

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
