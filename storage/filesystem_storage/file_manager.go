package filesystem_storage

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/customerio/homework/custom_error"
	"github.com/customerio/homework/global"
	"github.com/customerio/homework/models"
	"io/fs"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

const userStateDirectory = "/tmp/go/"
const resumeMarkerFilePath = userStateDirectory + "marker"
const offsetMarkerFilePath = userStateDirectory + "offset"

// intentionally not exported
type fileStorageManager struct {
	offsetFileHandle *os.File
}

// ignore non exported return type warning
func New() *fileStorageManager {
	return &fileStorageManager{}
}

func (fms *fileStorageManager) DeleteReportFile(ctx context.Context) error {
	_, err := os.Stat(ctx.Value(global.OutputFilePathKey).(string))
	if err == nil {
		err = os.Remove(ctx.Value(global.OutputFilePathKey).(string))
		if err != nil {
			return custom_error.New("Error deleting report file", err).Log()
		}
	}

	return nil
}

func (fms *fileStorageManager) ClearTempStorage() {
	err := os.RemoveAll(userStateDirectory)
	if err != nil {
		log.Println(custom_error.New("Error clearing temp filesystem_storage dir", err))
	}

	err = os.Mkdir(userStateDirectory, os.ModePerm)
	if err != nil {
		log.Println(custom_error.New("Error recreating tmp filesystem_storage dir after delete", err))
	}
}

func (fms *fileStorageManager) CheckReportFileExist(ctx context.Context) bool {
	_, err := os.Stat(ctx.Value(global.OutputFilePathKey).(string))
	return err == nil
}

func (fms *fileStorageManager) MoveToReportEnd(ctx context.Context) (int, *os.File) {
	byteArray, err := os.ReadFile(ctx.Value(global.OutputFilePathKey).(string))
	if err != nil {
		log.Println(custom_error.New("error reading existing report file", err))
		return -1, nil
	}

	var userId int
	//from end of file walk backwards to find beginning of last line
	for i := len(byteArray) - 2; i > 0; i-- {
		if byteArray[i] == '\n' {
			lastLine := string(byteArray[i+1])
			indexFirstComma := strings.Index(lastLine, ",")
			if indexFirstComma < 0 {
				log.Println("no comma in last line")
				return -1, nil
			}

			userId, err = strconv.Atoi(lastLine[0:indexFirstComma])
			if err != nil {
				log.Println(custom_error.New("error parsing last userId in report", err))
				return -1, nil
			}
		}
	}

	reportFileHandle, err := os.OpenFile(ctx.Value(global.OutputFilePathKey).(string), os.O_RDWR, 0666)
	if err != nil {
		log.Println(custom_error.New("error opening existing report file", err))
		return -1, nil
	}
	//move file pointer to end of file
	_, _ = reportFileHandle.Seek(0, 2)

	return userId, reportFileHandle
}

func (fms *fileStorageManager) LoadUserState(userId int) (*models.User, error) {
	id := strconv.Itoa(userId)

	//open file and read json
	byteArray, err := os.ReadFile(userStateDirectory + id)
	if err != nil {
		//File DNE
		if errors.Is(err, os.ErrNotExist) {
			return &models.User{
				ID:         userId,
				Attributes: map[string]*models.Attribute{},
				Events:     map[string]*models.Event{},
			}, nil
		}
		msg := "Error loading user state from file " + userStateDirectory + id
		return nil, custom_error.New(msg, err).Log()
	}

	// json -> user
	user := models.User{}
	err = json.Unmarshal(byteArray, &user)
	if err != nil {
		return nil, custom_error.New("Error unmarshaling userId "+id, err).Log()
	}

	if user.Events == nil {
		user.Events = map[string]*models.Event{}
	}

	if user.Attributes == nil {
		user.Attributes = map[string]*models.Attribute{}
	}

	return &user, nil
}

func (fms *fileStorageManager) SaveUserState(user *models.User) error {
	//create and open file
	fileHandle, err := os.Create(userStateDirectory + strconv.Itoa(user.ID))
	if err != nil {
		_ = fileHandle.Close()
		msg := "Error saving user state for userId: " + strconv.Itoa(user.ID)
		return custom_error.New(msg, err).Log()
	}

	// user -> json
	byteArray, err := json.Marshal(user)
	if err != nil {
		_ = fileHandle.Close()
		msg := "Error marshaling user state for userId: " + strconv.Itoa(user.ID)
		return custom_error.New(msg, err).Log()
	}

	//save json to file
	_, err = fileHandle.WriteString(string(byteArray))
	_ = fileHandle.Close()
	if err != nil {
		msg := "Error writing user state for userId: " + strconv.Itoa(user.ID)
		return custom_error.New(msg, err).Log()
	}

	return nil
}

func (fms *fileStorageManager) LoadAllUserIdsSorted() ([]int, error) {
	f, err := os.Open(userStateDirectory)
	if err != nil {
		return nil, custom_error.New("error opening tmp filesystem_storage dir "+userStateDirectory, err).Log()
	}
	//to prevent warning about unused err
	// Non obfuscated version:
	// defer f.close()
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
		}
	}(f)

	//get directory listing
	dirs, err := f.ReadDir(-1)
	if err != nil {
		return nil, custom_error.New("error getting dir listing "+userStateDirectory, err).Log()
	}

	//convert from []DirEntry to []int for sorting
	ids := make([]int, len(dirs))
	for i := 0; i < len(dirs); i++ {
		//handle hidden files like .DS_Store on MacOS
		if dirs[i].Name()[0] == '.' || dirs[i].Name() == "marker" || dirs[i].Name() == "offset" {
			continue
		}
		id, err := strconv.Atoi(dirs[i].Name())
		if err != nil {
			log.Println(custom_error.New("error converting "+dirs[i].Name()+" to int", err))
			continue
		}
		ids[i] = id
	}

	//sort
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return ids, nil
}

func CreateInterruptedMarkerFile() {
	err := os.MkdirAll(userStateDirectory, fs.ModePerm)
	if err != nil {
		log.Println(custom_error.New("error creating temp files directory", err))
	}

	_, err = os.Create(resumeMarkerFilePath)
	if err != nil {
		log.Println(custom_error.New("error creating interrupted marker file", err))
	}
}

// check for marker file existance to know if we start clean
// or resume from interruption
func WasInterrupted() bool {
	_, err := os.Stat(resumeMarkerFilePath)
	//not handling error condition since it will happen every time the file is not found
	//i.e we werent interrupted
	return err == nil
}

// used for resume from interruption functionality
// while building history from records, this saves the offset
// of the current record so we know where to resume from
func (fms *fileStorageManager) SetCurrentRecordOffset(offset int64) {
	if fms.offsetFileHandle == nil {
		var err error
		fms.offsetFileHandle, err = os.Create(offsetMarkerFilePath)
		if err != nil {
			log.Println(custom_error.New("error creating offset marker file "+offsetMarkerFilePath, err))
		}
	}

	_, err := fms.offsetFileHandle.WriteAt([]byte(strconv.FormatInt(offset, 10)), 0)
	if err != nil {
		log.Println(custom_error.New("Error setting current record offset", err))
	}
}

func (fms *fileStorageManager) GetCurrentRecordOffset() (int64, error) {
	var byteArray []byte
	if fms.offsetFileHandle == nil {
		var err error
		byteArray, err = os.ReadFile(offsetMarkerFilePath)
		if err != nil {
			return 0, custom_error.New("error getting current record offset from ReadFile", err).Log()
		}
	} else {
		_, err := fms.offsetFileHandle.ReadAt(byteArray, 0)
		if err != nil {
			return 0, custom_error.New("error seeking to offset 0", err).Log()
		}
	}

	offset, err := strconv.ParseInt(string(byteArray), 10, 64)
	if err != nil {
		return 0, custom_error.New("error parsing current record offset", err).Log()
	}

	return offset, nil
}

func (fms *fileStorageManager) CheckRecordOffsetExist() bool {
	_, err := os.Stat(offsetMarkerFilePath)
	//dont handle err as it occurs every time file exist = false
	return err == nil
}

func (fms *fileStorageManager) RemoveOffsetFile() {
	fms.offsetFileHandle = nil
	err := os.Remove(offsetMarkerFilePath)
	if err != nil {
		log.Println(custom_error.New("error deleting offset file", err))
	}
}
