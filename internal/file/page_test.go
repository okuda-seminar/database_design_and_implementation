package file

import (
	"testing"
)

func TestPageOperations(t *testing.T) {
	blockSize := 64
	page := NewPage(blockSize)

	// Test SetInt and GetInt
	err := page.SetInt(0, 1234)
	if err != nil {
		t.Fatalf("SetInt failed: %v", err)
	}

	value, err := page.GetInt(0)
	if err != nil {
		t.Fatalf("GetInt failed: %v", err)
	}
	if value != 1234 {
		t.Errorf("Expected 1234, got %d", value)
	}

	// Test out of range SetInt
	err = page.SetInt(blockSize-3, 5678)
	if err == nil {
		t.Errorf("Expected error for out of range SetInt, but got nil")
	}

	// Test SetBytes and GetBytes
	data := []byte("hello")
	err = page.SetBytes(4, data)
	if err != nil {
		t.Fatalf("SetBytes failed: %v", err)
	}

	retrievedData, err := page.GetBytes(4)
	if err != nil {
		t.Fatalf("GetBytes failed: %v", err)
	}
	if string(retrievedData) != "hello" {
		t.Errorf("Expected 'hello', got '%s'", retrievedData)
	}

	// Test out of range SetBytes
	largeData := make([]byte, blockSize)
	err = page.SetBytes(10, largeData)
	if err == nil {
		t.Errorf("Expected error for out of range SetBytes, but got nil")
	}

	// Test SetString and GetString
	err = page.SetString(10, "world")
	if err != nil {
		t.Fatalf("SetString failed: %v", err)
	}

	retrievedStr, err := page.GetString(10)
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if retrievedStr != "world" {
		t.Errorf("Expected 'world', got '%s'", retrievedStr)
	}

	// Test out of range GetString
	_, err = page.GetString(blockSize - 3)
	if err == nil {
		t.Errorf("Expected error for out of range GetString, but got nil")
	}

	// Test MaxLength
	maxLen := MaxLength(10)
	expectedLen := 10 + intSize
	if maxLen != expectedLen {
		t.Errorf("Expected %d, got %d", expectedLen, maxLen)
	}

	// Test Contents
	contents := page.Contents()
	if len(contents) != blockSize {
		t.Errorf("Expected %d, got %d", blockSize, len(contents))
	}
}
