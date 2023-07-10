package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/customerio/homework/custom_error"
	"github.com/customerio/homework/global"
	"github.com/customerio/homework/report"
	"github.com/customerio/homework/storage"
	"github.com/customerio/homework/storage/filesystem_storage"
	"github.com/customerio/homework/storage/memory_storage"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const uo = "\033[4m"
const uc = "\033[0m"

const flagHelpMessage = "usage:  \n    " +
	"go run . [-i " + uo + "input_file_path" + uc + "]  " +
	"[-o " + uo + "output_file_path" + uc + "]  " +
	"[-c " + uo + "verify_file_path" + uc + "]  " +
	"[-s " + uo + "memory|filesystem" + uc + "]  " +
	"[-t " + uo + "tmp_dir_path" + uc + "]\n" +
	"    go run . -h \n\n" +
	"-i " + uo + "input_file_path" + uc + ", --input=" + uo + "input_file_path" + uc + "\n" +
	"    Path to file used as input for report generation\n" +
	"-o " + uo + "output_file_path" + uc + ", --output=" + uo + "output_file_path" + uc + "\n" +
	"    File path to be used for output file\n" +
	"-c " + uo + "control_file_path" + uc + ", --control=" + uo + "control_file_path" + uc + "\n" +
	"    Path to file used as verification of report output\n" +
	"-s " + uo + "memory|filesystem" + uc + ", --storage=" + uo + "memory|filesystem" + uc + "\n" +
	"    Storage method for temporary data.  Either \"memory\" or \"filesystem\"\n" +
	"-t " + uo + "tmp_dir_path" + uc + ", --tmp=" + uo + "tmp_dir_path" + uc + "\n" +
	"    Directory for temporary data.  Ignored if -s is not set to \"filesystem\"\n" +
	"-h , --help \n" +
	"    Display this message\n\n\n"

func main() {
	ctx := parseArgs(context.Background())
	ctx, cancel := context.WithCancel(ctx)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	var storageManager storage.StorageManager
	if ctx.Value(global.StorageSystemKey) == global.FilesystemStorageType() {
		storageManager = filesystem_storage.New()
		//stuff for resume functionality
		ctx = context.WithValue(ctx, global.InterruptedKey, filesystem_storage.WasInterrupted())
		filesystem_storage.CreateInterruptedMarkerFile()
	} else {
		storageManager = memory_storage.New()
		ctx = context.WithValue(ctx, global.InterruptedKey, false)
	}

	err := report.GenerateReport(ctx, storageManager)
	storageManager.ClearTempStorage()
	if err != nil {
		log.Fatal(custom_error.New("Error generating report", err))
	}

	if err := ctx.Err(); err != nil {
		log.Fatal(err)
	}

	err = validate(ctx.Value(global.OutputFilePathKey).(string), ctx.Value(global.VerifyFilePathKey).(string))
	if err != nil {
		log.Fatal(custom_error.New("Error validating report", err))
	}

	log.Println("SUCCESS")

	os.Exit(0)
}

func parseArgs(ctx context.Context) context.Context {
	var inputFilePath string
	var outputFilePath string
	var verifyFilePath string
	var storageSystem string
	var tmpDir string

	flag.StringVar(&inputFilePath, "i", "data/messages.1.data", "file path of input file")
	flag.StringVar(&inputFilePath, "input", "data/messages.1.data", "file path of input file")
	flag.StringVar(&outputFilePath, "o", "data/report.txt", "file path of output file")
	flag.StringVar(&outputFilePath, "output", "data/report.txt", "file path of output file")
	flag.StringVar(&verifyFilePath, "c", "data/verify.1.csv", "file path of control / verify file")
	flag.StringVar(&verifyFilePath, "control", "data/verify.1.csv", "file path of control / verify file")
	flag.StringVar(&storageSystem, "s", "memory", "Tmp data storage system, \"memory\" or \"filesystem\"")
	flag.StringVar(&storageSystem, "storage", "memory", "Tmp data storage system, \"memory\" or \"filesystem\"")
	flag.StringVar(&tmpDir, "t", "/tmp/go", "Tmp file storage directory path")
	flag.StringVar(&tmpDir, "tmp", "/tmp/go", "Tmp file storage directory path")

	flag.Usage = func() {
		fmt.Print(flagHelpMessage)
	}

	flag.Parse()

	ctx = context.WithValue(ctx, global.InputFilePathKey, inputFilePath)
	ctx = context.WithValue(ctx, global.OutputFilePathKey, outputFilePath)
	ctx = context.WithValue(ctx, global.VerifyFilePathKey, verifyFilePath)
	ctx = context.WithValue(ctx, global.StorageSystemKey, global.StorageType(storageSystem))
	ctx = context.WithValue(ctx, global.TmpDirKey, tmpDir)

	return ctx
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
