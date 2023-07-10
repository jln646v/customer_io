package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/customerio/homework/custom_error"
	"github.com/customerio/homework/global"
	"github.com/customerio/homework/report"
	"github.com/customerio/homework/storage"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const _dataFilePattern = "data/messages.%s.data"
const _verifyFilePattern = "data/verify.%s.csv"

func main() {
	if len(os.Args) != 2 {
		log.Fatal(fmt.Sprintf("Incorrect num of args!  Expected: 1 Found: %d", len(os.Args)-1))
	}

	if os.Args[1] != "1" && os.Args[1] != "2" && os.Args[1] != "3" {
		log.Fatal("unknown or invalid argument: " + os.Args[1])
	}

	global.InputFilePath = fmt.Sprintf(_dataFilePattern, os.Args[1])
	verifyFile := fmt.Sprintf(_verifyFilePattern, os.Args[1])
	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	global.WasInterrupted = storage.WasInterrupted()
	_ = storage.CreateInterruptedMarkerFile()

	err := report.GenerateReport(ctx)
	if err != nil {
		log.Fatal(custom_error.New("Error generating report", err))
	}

	if global.UseStorage {
		err := storage.ClearTempStorage()
		if err != nil {
			log.Println(custom_error.New("error clearing tmp storage", err))
		}
	}

	if err := ctx.Err(); err != nil {
		log.Fatal(err)
	}

	err = validate(global.ReportFilePath, verifyFile)
	if err != nil {
		log.Fatal(custom_error.New("Error validating report", err))
	}

	log.Println("SUCCESS")

	os.Exit(0)
}

// Quick validation of expected and received input.
func validate(have, want string) error {
	f1, err := os.Open(have)
	if err != nil {
		return err
	}
	defer func(f1 *os.File) {
		err := f1.Close()
		if err != nil {

		}
	}(f1)

	f2, err := os.Open(want)
	if err != nil {
		return err
	}
	defer func(f2 *os.File) {
		err := f2.Close()
		if err != nil {

		}
	}(f2)

	s1 := bufio.NewScanner(f1)
	s2 := bufio.NewScanner(f2)
	for s1.Scan() {
		if !s2.Scan() {
			return fmt.Errorf("want: insufficient data")
		}
		t1 := s1.Text()
		t2 := s2.Text()
		if t1 != t2 {
			return fmt.Errorf("have/want: difference\n%s\n%s", t1, t2)
		}
	}
	if s2.Scan() {
		return fmt.Errorf("have: insufficient data")
	}
	if err := s1.Err(); err != nil {
		return err
	}
	if err := s2.Err(); err != nil {
		return err
	}
	return nil
}
