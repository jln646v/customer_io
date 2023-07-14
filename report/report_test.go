package report

import (
	"fmt"
	"github.com/customerio/homework/models"
	"testing"
)

func TestPrintUserEntry(t *testing.T) {
	userId := 123
	attr0Key := "attr0"
	attr1Key := "attr1"
	attr0Value := "value0"
	attr1Value := "value1"
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
	}
	attr0 := models.Attribute{
		Timestamp: 123456,
		Value:     attr0Value,
	}
	attr1 := models.Attribute{
		Timestamp: 45612,
		Value:     attr1Value,
	}
	events := map[string]*models.Event{
		event0Key: &event0,
		event1Key: &event1}
	attrs := map[string]*models.Attribute{
		attr0Key: &attr0,
		attr1Key: &attr1}

	user := models.User{
		ID:         userId,
		Events:     events,
		Attributes: attrs,
	}

	expected := fmt.Sprintf("%d,%s=%s,%s=%s,%s=%d,%s=%d", userId, attr0Key, attr0Value,
		attr1Key, attr1Value, event0Key, event0Num, event1Key, event1Num)
	result := printUserEntry(&user)

	if result != expected {
		t.Errorf("UserEntry line incorrect.  Expected: %s  Actual: %s", expected, result)
	}
}
