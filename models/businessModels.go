package models

type HistoryType string

const (
	AttributeType HistoryType = "attributes"
	EventType     HistoryType = "event"
)

type User struct {
	ID         int
	Attributes map[string]*Attribute
	Events     map[string]*Event
}

type UserHistory struct {
	UserId      int
	Attributes  map[string]*Attribute
	Event       *Event
	HistoryType HistoryType
}

type Attribute struct {
	Timestamp int64
	Value     string
}

type Event struct {
	NumOccurrances int
	Ids            map[string]struct{}
	Name           string
}
