package global

const InputFilePathKey = "inputFilePath"
const OutputFilePathKey = "outputFilePath"
const VerifyFilePathKey = "verifyFilePath"
const StorageSystemKey = "storageSystemType"
const TmpDirKey = "tmpDir"
const InterruptedKey = "interrupted"

type storageType string

const (
	memory     storageType = "memory"
	fileSystem storageType = "filesystem"
)

func MemoryStorageType() storageType {
	return memory
}

func FilesystemStorageType() storageType {
	return fileSystem
}

func StorageType(str string) storageType {
	switch str {
	case "memory":
		return MemoryStorageType()
	case "filesystem":
		return FilesystemStorageType()
	default:
		return MemoryStorageType()
	}
}
