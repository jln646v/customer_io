package stream

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/customerio/homework/custom_error"
	"github.com/customerio/homework/global"
	"github.com/customerio/homework/models"
	"io"
	"log"
	"os"
	"strconv"
)

type RecordType string

const (
	Attributes RecordType = "attributes"
	Event      RecordType = "event"
)

type Record struct {
	ID        string            `json:"id"`
	Type      RecordType        `json:"type"`
	Name      string            `json:"name"`
	UserID    string            `json:"user_id"`
	Data      map[string]string `json:"data"`
	Timestamp int64             `json:"timestamp"`

	// Position in the input stream where this record lives.
	Position int64 `json:"-"`
}

// Process returns a channel to which a stream of records are sent. Reading starts at
// the current seek offset in the file. The channel is closed when no more records are available.
// If the context completes, reading is prematurely terminated.
func process(ctx context.Context, f io.ReadSeeker) (<-chan *Record, error) {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	ch := make(chan *Record)
	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(f)
		scanner.Split(func(data []byte, atEof bool) (advance int, token []byte, err error) {
			advance, token, err = bufio.ScanLines(data, atEof)
			if err == nil && token != nil {
				offset += int64(advance)
			}
			return advance, token, err
		})
		for scanner.Scan() {
			rec := &Record{
				Position: offset,
			}
			if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
				log.Println("json.Unmarshal failed", err)
				continue
			}
			select {
			case _ = <-ctx.Done():
				return
			case ch <- rec:
			}
		}
	}()
	return ch, nil
}

func GetRecords(ctx context.Context) (<-chan *Record, error) {
	inputFilePath := ctx.Value(global.InputFilePathKey).(string)
	f, err := os.Open(inputFilePath)
	if err != nil {
		return nil, custom_error.New("Error opening file: "+inputFilePath, err).Log()
	}

	seeker := io.ReadSeeker(f)

	ch, err := process(ctx, seeker)
	if err != nil {
		return nil, custom_error.New("error processing stream from "+inputFilePath, err).Log()
	}

	return ch, nil
}

func Map(rec *Record) (*models.UserHistory, error) {
	userId, err := strconv.Atoi(rec.UserID)
	if err != nil {
		return nil, err
	}

	userHistory := models.UserHistory{
		UserId:     userId,
		Attributes: map[string]*models.Attribute{},
	}

	if rec.Type == Attributes {
		userHistory.HistoryType = models.AttributeType
		for key, value := range rec.Data {
			attribute := models.Attribute{
				Value:     value,
				Timestamp: rec.Timestamp}

			userHistory.Attributes[key] = &attribute
		}

	} else if rec.Type == Event {
		userHistory.HistoryType = models.EventType
		userHistory.Event = &models.Event{Name: rec.Name, Ids: map[string]struct{}{}}
		userHistory.Event.Ids[rec.ID] = struct{}{}
		userHistory.Event.NumOccurrances = 1
	}

	return &userHistory, nil
}
