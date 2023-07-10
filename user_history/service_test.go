package user_history

import (
	"github.com/customerio/homework/models"
	"testing"
)

func TestAddEvent(t *testing.T) {
	event0Key := "event0"
	event1Key := "event1"
	event0Num := 4
	event1Num := 7

	event0 := models.Event{
		NumOccurrances: event0Num,
		Ids:            map[string]struct{}{"id0": {}},
	}
	event1 := models.Event{
		NumOccurrances: event1Num,
		Ids:            map[string]struct{}{"id1": {}},
		Name:           event1Key,
	}
	userEvents := map[string]*models.Event{
		event0Key: &event0}

	addEvent(userEvents, &event1)

	_, ok := userEvents[event1Key]
	if !ok {
		t.Errorf("failed to add event to userEvents map")
	}
}

func TestAddAttributes(t *testing.T) {

}
