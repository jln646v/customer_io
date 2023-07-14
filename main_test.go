package main

import (
	"context"
	"github.com/customerio/homework/global"
	"os"
	"testing"
)

func TestParseArgs(t *testing.T) {
	inputFilePath := "inputFilePath"
	outputFilePath := "outputFilePath"
	controlFilePath := "controlFilePath"
	storage := "filesystem"
	tmpDirPath := "tmpDirPath"
	os.Args = []string{"cmd", "-i", inputFilePath, "-o", outputFilePath, "-c", controlFilePath,
		"-s", storage, "-t", tmpDirPath}

	ctx := parseArgs(context.Background())

	if ctx.Value(global.InputFilePathKey).(string) != inputFilePath {
		t.Errorf("InputFilePath incorrect.  Wanted: %s  Actual: %s",
			inputFilePath, ctx.Value(global.InputFilePathKey).(string))
	}

	if ctx.Value(global.OutputFilePathKey).(string) != outputFilePath {
		t.Errorf("OutputFilePath incorrect  Wanted: %s  Actual: %s",
			outputFilePath, ctx.Value(global.OutputFilePathKey).(string))
	}

	if ctx.Value(global.VerifyFilePathKey).(string) != controlFilePath {
		t.Errorf("ControlFilePath incorrect.  Wanted: %s  Actual: %s",
			controlFilePath, ctx.Value(global.VerifyFilePathKey).(string))
	}

	if ctx.Value(global.StorageSystemKey) != global.StorageType(storage) {
		t.Errorf("ControlFilePath incorrect.  Wanted: %s  Actual: %s",
			storage, ctx.Value(global.StorageSystemKey))
	}

	if ctx.Value(global.TmpDirKey).(string) != tmpDirPath {
		t.Errorf("TmpDirPath incorrect.  Wanted: %s  Actual: %s",
			tmpDirPath, ctx.Value(global.TmpDirKey).(string))
	}

}
