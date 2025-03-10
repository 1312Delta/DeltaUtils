package file

import (
	"hash"
	"io"
	"log"
	"os"
)

func OpenOrCreateFile(name string) *os.File {
	if !fileExists(name) {
		createFile(name)
	}
	file := openFile(name)
	return file
}

func openFile(name string) *os.File {
	file, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	return file
}

func fileExists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}

func createFile(name string) {
	_, err := os.Create(name)
	if err != nil {
		log.Fatal(err)
		return
	}
}

func DeleteFile(name string) error {
	// Try to open the file to check if it is open
	file, err := os.Open(name)
	if err == nil {
		// If the file is open, close it
		if err := file.Close(); err != nil {
			return err
		}
	}

	// Delete the file
	if err := os.Remove(name); err != nil {
		return err
	}
	return nil
}

func GetFileHash(path string, h hash.Hash) []byte {
	file := openFile(path)
	_, err := io.Copy(h, file)
	if err != nil {
		log.Println(err)
		return nil
	}
	hs := h.Sum(nil)
	return hs
}

func GetFileHashString(path string, h hash.Hash) string {
	hs := GetFileHash(path, h)
	return string(hs)
}
