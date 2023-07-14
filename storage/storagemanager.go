package storage

import (
	"context"
	"github.com/customerio/homework/models"
	"os"
)

type StorageManager interface {
	DeleteReportFile(ctx context.Context) error
	GetCurrentRecordOffset() (int64, error)
	CheckRecordOffsetExist() bool
	SetCurrentRecordOffset(offset int64)
	LoadUserState(userId int) (*models.User, error)
	SaveUserState(user *models.User) error
	RemoveOffsetFile()
	LoadAllUserIdsSorted() ([]int, error)
	CheckReportFileExist(ctx context.Context) bool
	MoveToReportEnd(ctx context.Context) (int, *os.File)
	ClearTempStorage()
}
