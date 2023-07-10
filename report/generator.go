package report

import (
	"context"
	"fmt"
	"github.com/customerio/homework/custom_error"
	"github.com/customerio/homework/global"
	"github.com/customerio/homework/models"
	"github.com/customerio/homework/storage"
	"github.com/customerio/homework/stream"
	"github.com/customerio/homework/user_history"
	"log"
	"strconv"
	"strings"
)

func GenerateReport(ctx context.Context) error {
	if !global.WasInterrupted {
		err := storage.DeleteReportFile()
		if err != nil {
			return custom_error.New("Error deleting report file", err).Log()
		}
	}

	recordStream, err := stream.GetRecords(ctx)
	if err != nil {
		return custom_error.New("error getting record stream", err).Log()
	}

	//create users with their associated Events and Attributes
	//global.UseStorage saves user files to disk
	//non global.UseStorage maintains entire list in memory
	var userHistories map[int]*models.User
	if global.UseStorage {
		_, err = user_history.CreateHistories(recordStream)
	} else {
		userHistories, err = user_history.CreateHistories(recordStream)
	}
	if err != nil {
		return custom_error.New("error updating histories", err).Log()
	}

	var sortedUserIds []int
	var restoreLastProcessedUserId int
	//get list of user ids numerically sorted
	if global.UseStorage {
		sortedUserIds, err = storage.LoadAllUserIds()
		if err != nil {
			return custom_error.New("error getting sorted user ids from storage", err).Log()
		}

		if storage.CheckReportFileExist() {
			restoreLastProcessedUserId = storage.MoveToReportEnd()
		}
	} else {
		sortedUserIds = user_history.SortUserIds(userHistories)
	}

	err = printReportForEachUser(sortedUserIds, userHistories, restoreLastProcessedUserId)

	err = storage.CloseReportFile()
	if err != nil {
		log.Println(custom_error.New("error closing Report File", err))
	}

	return nil
}

func printReportForEachUser(
	sortedUserIds []int,
	userHistories map[int]*models.User,
	restoreLastProcessedUserId int) error {

	for _, userId := range sortedUserIds {
		//on interruption restore, skip along sortedUserIds until we get to
		//the one next after last written to report
		if restoreLastProcessedUserId >= userId {
			continue
		}

		var user *models.User
		if global.UseStorage {
			var err error
			user, err = storage.LoadUserState(userId)
			if err != nil {
				log.Println(
					custom_error.New(
						fmt.Sprintf("error loading state userId: %d", userId), err))
				continue
			}
		} else {
			user = userHistories[userId]
		}

		err := storage.AddLineToReport(printUserEntry(user) + "\n")
		if err != nil {
			return custom_error.New(
				fmt.Sprintf("error writing user to Report file.  UserId: %d", userId), err).Log()
		}
	}

	return nil
}

func printUserEntry(user *models.User) string {
	var sb strings.Builder

	sb.WriteString(strconv.Itoa(user.ID))
	sb.WriteString(",")

	printUserAttributesEntry(user, &sb)
	printUserEventEntry(user, &sb)

	entry := sb.String()
	return entry[0 : len(entry)-1]
}

func printUserAttributesEntry(user *models.User, sb *strings.Builder) {
	sortedAttributes := user_history.SortAttributes(user.Attributes)
	for _, attrKey := range sortedAttributes {
		sb.WriteString(attrKey)
		sb.WriteString("=")
		sb.WriteString(user.Attributes[attrKey].Value)
		sb.WriteString(",")
	}
}

func printUserEventEntry(user *models.User, sb *strings.Builder) {
	sortedEvents := user_history.SortEvents(user.Events)

	for _, eventName := range sortedEvents {
		sb.WriteString(eventName)
		sb.WriteString("=")
		sb.WriteString(strconv.Itoa(user.Events[eventName].NumOccurrances))
		sb.WriteString(",")
	}
}
