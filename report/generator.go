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
	"os"
	"strconv"
	"strings"
)

var reportFileHandle *os.File

func GenerateReport(ctx context.Context, storageManager storage.StorageManager) error {
	err := storageManager.DeleteReportFile(ctx)
	if err != nil {
		return custom_error.New("Error deleting report file", err).Log()
	}

	recordStream, err := stream.GetRecords(ctx)
	if err != nil {
		return custom_error.New("error getting record stream", err).Log()
	}

	//create users with their associated Events and Attributes
	user_history.CreateHistories(ctx, recordStream, storageManager)

	//get list of user ids numerically sorted
	sortedUserIds, err := storageManager.LoadAllUserIdsSorted()
	if err != nil {
		return custom_error.New("error getting sorted user ids", err).Log()
	}

	var restoreLastProcessedUserId = 0
	if storageManager.CheckReportFileExist(ctx) {
		restoreLastProcessedUserId, reportFileHandle = storageManager.MoveToReportEnd(ctx)
	}

	return printReportForEachUser(ctx, sortedUserIds, storageManager, restoreLastProcessedUserId)
}

func printReportForEachUser(
	ctx context.Context,
	sortedUserIds []int,
	storageManager storage.StorageManager,
	restoreLastProcessedUserId int) error {
	for _, userId := range sortedUserIds {
		//on interruption restore, skip along sortedUserIds until we get to
		//the one next after last written to report
		if restoreLastProcessedUserId >= userId {
			continue
		}

		user, err := storageManager.LoadUserState(userId)
		if err != nil {
			log.Println(
				custom_error.New(
					fmt.Sprintf("error loading state userId: %d", userId), err))
			continue
		}

		err = addLineToReport(ctx, printUserEntry(user)+"\n")
		if err != nil {
			return custom_error.New(
				fmt.Sprintf("error writing user to Report file.  UserId: %d", userId), err).Log()
		}
	}

	_ = reportFileHandle.Close()

	return nil
}

func addLineToReport(ctx context.Context, line string) error {
	if reportFileHandle == nil {
		var err error
		reportFileHandle, err = os.Create(ctx.Value(global.OutputFilePathKey).(string))
		if err != nil {
			return custom_error.New("Error creating report file", err).Log()
		}
	}

	_, err := reportFileHandle.WriteString(line)
	if err != nil {
		return custom_error.New(fmt.Sprintf("Error writing line to report: %s", line), err).Log()
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
	//remove trailing ","
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
