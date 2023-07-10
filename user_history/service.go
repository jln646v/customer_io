package user_history

import (
	"github.com/customerio/homework/custom_error"
	"github.com/customerio/homework/global"
	"github.com/customerio/homework/models"
	"github.com/customerio/homework/storage"
	"github.com/customerio/homework/stream"
	"log"
	"strconv"
)

// CreateHistories loop over stream from input file
// creating list (in memory or on disk) of users and their associated Events and Attributes
func CreateHistories(recordStream <-chan *stream.Record) (map[int]*models.User, error) {
	var users map[int]*models.User
	var resumeOffset int64

	if global.UseStorage && global.WasInterrupted {
		if !storage.CheckRecordOffsetExist() {
			return users, nil
		} else {
			resumeOffset, _ = storage.GetCurrentRecordOffset()
		}
	} else {
		users = map[int]*models.User{}
	}

	for rec := range recordStream {
		//skip to last record seen
		if global.UseStorage && (resumeOffset > rec.Position) {
			continue
		} else if global.UseStorage {
			_ = storage.SetCurrentRecordOffset(rec.Position)
		}

		userId, _ := strconv.Atoi(rec.UserID)
		userHistory, err := stream.Map(rec)
		if err != nil {
			log.Println(custom_error.New("error mapping record from stream", err))
			continue
		}

		if global.UseStorage {
			user, err := storage.LoadUserState(userId)
			if err != nil {
				log.Println(custom_error.New("error loading state for userId: "+rec.UserID, err))
				continue
			}
			users = map[int]*models.User{}
			users[userId] = user
		}

		//populate user with event/attr info
		err = add(users, userHistory)
		if err != nil {
			log.Println(custom_error.New("error adding userHistory", err))
			continue
		}

		//save user to storage
		if global.UseStorage {
			err = storage.SaveUserState(users[userId])
			if err != nil {
				msg := "error saving user state to storage for userId: " + rec.UserID
				log.Println(custom_error.New(msg, err))
				continue
			}
		}
	}

	if global.UseStorage {
		storage.RemoveOffsetFile()
	}

	return users, nil
}

// Dedupe Users
// populate user with history data
func add(users map[int]*models.User, userHistory *models.UserHistory) error {
	//create user if it is first time encountering this userId
	user, ok := users[userHistory.UserId]
	if !ok {
		user = &models.User{
			ID:         userHistory.UserId,
			Attributes: map[string]*models.Attribute{},
			Events:     map[string]*models.Event{},
		}
		users[userHistory.UserId] = user
	}

	//we have different parsing for Events and Attributes, handle accordingly
	if userHistory.HistoryType == models.AttributeType {
		addAttributes(user.Attributes, userHistory.Attributes)
	} else if userHistory.HistoryType == models.EventType {
		addEvent(user.Events, userHistory.Event)
	} else { //Should never happen
		return custom_error.New("Unknown HistoryType userId: "+strconv.Itoa(user.ID), nil).Log()
	}

	return nil
}

// decorate user attributes
func addAttributes(userAttrs map[string]*models.Attribute, historyAttrs map[string]*models.Attribute) {
	for historyKey, historyElement := range historyAttrs {
		userAttrElement, ok := userAttrs[historyKey]

		//if we have not encountered this attribute type yet,
		// or we have, but this timestamp is the most recent for that type
		if !ok || (historyElement.Timestamp > userAttrElement.Timestamp) {
			userAttrs[historyKey] = historyElement
		}
	}
}

func addEvent(userEvents map[string]*models.Event, historyEvent *models.Event) {
	userEvent, ok := userEvents[historyEvent.Name]
	//first time we are encountering this event type
	if !ok {
		userEvents[historyEvent.Name] = historyEvent

	} else {
		//handle duplicate event ids
		for id := range historyEvent.Ids {
			_, ok := userEvent.Ids[id]
			if ok { //duplicate event
				return
			} else { //not a duplicate event id, but we have seen this event type before
				userEvent.Ids[id] = struct{}{}
				userEvent.NumOccurrances++
			}
		}
	}
}
