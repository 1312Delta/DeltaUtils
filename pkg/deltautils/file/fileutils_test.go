package file

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

func TestOpenOrCreateFile(t *testing.T) {
	fileName := "test.txt"

	// Ensure the file does not exist before the test
	if _, err := os.Stat(fileName); err == nil {
		if err := DeleteFile(fileName); err != nil {
			t.Fatalf("Failed to delete test file: %v", err)
		}
	}

	file := OpenOrCreateFile(fileName)
	if file == nil {
		t.Fatalf("Expected file to be created, but got nil")
	}

	// Check if the file exists
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		t.Fatalf("Expected file %s to exist, but it does not", fileName)
	}

	// Clean up
	if err := file.Close(); err != nil {
		t.Fatalf("Failed to close test file: %v", err)
	}
	if err := DeleteFile(fileName); err != nil {
		t.Fatalf("Failed to delete test file: %v", err)
	}
}

func TestGetFileHash(t *testing.T) {
	fileName := "test.txt"
	expectedHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // SHA256 hash of an empty file

	// Ensure the file does not exist before the test
	if _, err := os.Stat(fileName); err == nil {
		if err := DeleteFile(fileName); err != nil {
			t.Fatalf("Failed to delete test file: %v", err)
		}
	}

	// Create an empty file for testing
	file, err := os.Create(fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Failed to close test file: %v", err)
	}

	// Calculate the hash of the file
	h := sha256.New()
	hashBytes := GetFileHash(fileName, h)

	// Convert the hash to a string
	hashString := hex.EncodeToString(hashBytes)

	// Check if the hash matches the expected value
	if hashString != expectedHash {
		t.Fatalf("Expected hash %s, but got %s", expectedHash, hashString)
	}

	//Test GetFileHashString too
	hashString = GetFileHashString(fileName, h)
	if hashString != expectedHash {
		t.Fatalf("Expected hash %s, but got %s", expectedHash, hashString)
	}

	// Clean up
	if err := DeleteFile(fileName); err != nil {
		t.Fatalf("Failed to delete test file: %v", err)
	}
}

func TestDeleteFile(t *testing.T) {
	fileName := "test.txt"

	// Ensure the file exists before the test
	file, err := os.Create(fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Failed to close test file: %v", err)
	}

	// Delete the file
	if err := DeleteFile(fileName); err != nil {
		t.Fatalf("Failed to delete test file: %v", err)
	}

	// Check if the file exists
	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		t.Fatalf("Expected file %s to be deleted, but it still exists", fileName)
	}
}
