package memory_storage

import (
	"context"
	"github.com/customerio/homework/models"
	"os"
	"sort"
)

// deliberatly not exported
type memoryStorageManager struct {
	users map[int]*models.User
}

// ignore non-exported return type warning
func New() *memoryStorageManager {
	return &memoryStorageManager{
		users: make(map[int]*models.User),
	}
}

func (*memoryStorageManager) DeleteReportFile(ctx context.Context) error {
	//NOOP
	return nil
}

func (*memoryStorageManager) GetCurrentRecordOffset() (int64, error) {
	//NOOP
	return 0, nil
}

func (*memoryStorageManager) CheckRecordOffsetExist() bool {
	//NOOP
	return false
}

func (*memoryStorageManager) SetCurrentRecordOffset(int64) {
	//NOOP
}

func (msm *memoryStorageManager) LoadUserState(userId int) (*models.User, error) {
	user, ok := msm.users[userId]
	if !ok {
		user = &models.User{
			ID:         userId,
			Attributes: map[string]*models.Attribute{},
			Events:     map[string]*models.Event{},
		}
	}
	return user, nil
}

func (msm *memoryStorageManager) SaveUserState(user *models.User) error {
	msm.users[user.ID] = user

	return nil
}

func (*memoryStorageManager) RemoveOffsetFile() {
	//NOOP
}

func (msm *memoryStorageManager) LoadAllUserIdsSorted() ([]int, error) {
	sorted := make([]int, len(msm.users))

	i := 0
	for userId := range msm.users {
		sorted[i] = userId
		i++
	}

	sort.Ints(sorted)

	return sorted, nil
}

func (*memoryStorageManager) CheckReportFileExist(ctx context.Context) bool {
	//NOOP
	return false
}

func (*memoryStorageManager) MoveToReportEnd(ctx context.Context) (int, *os.File) {
	//NOOP
	return 0, nil
}

func (msm *memoryStorageManager) ClearTempStorage() {
	msm.users = make(map[int]*models.User)
}
