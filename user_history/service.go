package user_history

import (
	"context"
	"fmt"
	"github.com/customerio/homework/custom_error"
	"github.com/customerio/homework/global"
	"github.com/customerio/homework/models"
	"github.com/customerio/homework/storage"
	"github.com/customerio/homework/stream"
	"log"
	"strconv"
)

// creating list of users and their associated Events and Attributes
func CreateHistories(ctx context.Context, recordStream <-chan *stream.Record, storageManager storage.StorageManager) {
	var resumeOffset int64 = 0

	if ctx.Value(global.InterruptedKey).(bool) {
		//resume functionality
		//move straight to writing report
		if !storageManager.CheckRecordOffsetExist() {
			return
		} else {
			resumeOffset, _ = storageManager.GetCurrentRecordOffset()
		}
	}

	for rec := range recordStream {
		//skip to last record seen if resuming
		if resumeOffset > rec.Position {
			continue
		}

		//marker for restore
		storageManager.SetCurrentRecordOffset(rec.Position)

		userId, err := strconv.Atoi(rec.UserID)
		if err != nil {
			log.Println(
				custom_error.New(
					fmt.Sprintf("error casting userId: %s from string to int", rec.UserID), err))
			continue
		}

		userHistory, err := stream.Map(rec)
		if err != nil {
			log.Println(custom_error.New("error mapping record from stream", err))
			continue
		}

		user, err := storageManager.LoadUserState(userId)
		if err != nil {
			log.Println(custom_error.New("error loading state for userId: "+rec.UserID, err))
			continue
		}

		//populate user with event/attr info
		err = addHistory(user, userHistory)
		if err != nil {
			log.Println(custom_error.New("error adding userHistory", err))
			continue
		}

		err = storageManager.SaveUserState(user)
		if err != nil {
			msg := "error saving user state for userId: " + rec.UserID
			log.Println(custom_error.New(msg, err))
			continue
		}
	}

	storageManager.RemoveOffsetFile()
}

// Dedupe Users
// populate user with history data
func addHistory(user *models.User, userHistory *models.UserHistory) error {
	//we have different parsing for Events and Attributes, handle accordingly
	if userHistory.HistoryType == models.AttributeType {
		addAttributes(user.Attributes, userHistory.Attributes)
	} else if userHistory.HistoryType == models.EventType {
		addEvent(user.Events, userHistory.Event)
	} else { //Should never happen
		return custom_error.New(
			fmt.Sprintf("Unknown HistoryType userId: %d", user.ID), nil).Log()
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
