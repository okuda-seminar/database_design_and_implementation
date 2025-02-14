package file

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// Define the int size
const intSize = 4

// Page represents a block of memory that can store data.
type Page struct {
	data []byte
}

// NewPage creates a new Page with a given block size.
func NewPage(blockSize int) *Page {
	return &Page{data: make([]byte, blockSize)}
}

// NewPageFromBytes creates a new Page from an existing byte slice (used for log pages).
func NewPageFromBytes(b []byte) *Page {
	return &Page{data: b}
}

// GetInt retrieves an integer from the specified offset.
func (p *Page) GetInt(offset int) (int32, error) {
	if offset+intSize > len(p.data) {
		return 0, errors.New("offset out of range")
	}
	return int32(binary.BigEndian.Uint32(p.data[offset:])), nil
}

// SetInt writes an integer to the specified offset.
func (p *Page) SetInt(offset int, n int32) error {
	if offset+intSize > len(p.data) {
		return errors.New("offset out of range")
	}
	binary.BigEndian.PutUint32(p.data[offset:], uint32(n))
	return nil
}

// GetBytes retrieves a byte array from the specified offset.
func (p *Page) GetBytes(offset int) ([]byte, error) {
	if offset+intSize > len(p.data) {
		return nil, errors.New("offset out of range")
	}
	length := int(binary.BigEndian.Uint32(p.data[offset:]))
	if offset+intSize+length > len(p.data) {
		return nil, errors.New("byte array out of range")
	}
	return p.data[offset+intSize : offset+intSize+length], nil
}

// SetBytes stores a byte array at the specified offset.
func (p *Page) SetBytes(offset int, b []byte) error {
	if offset+intSize+len(b) > len(p.data) {
		return errors.New("offset out of range")
	}
	binary.BigEndian.PutUint32(p.data[offset:], uint32(len(b)))
	copy(p.data[offset+intSize:], b)
	return nil
}

// GetString retrieves a string from the specified offset.
func (p *Page) GetString(offset int) (string, error) {
	b, err := p.GetBytes(offset)
	if err != nil {
		return "", err
	}
	return string(bytes.TrimRight(b, "\x00")), nil
}

// SetString stores a string at the specified offset.
func (p *Page) SetString(offset int, s string) error {
	b := []byte(s)
	return p.SetBytes(offset, b)
}

// MaxLength returns the maximum byte length required to store a string of given length.
func MaxLength(strlen int) int {
	return intSize + strlen
}

// Contents returns the raw byte slice.
func (p *Page) Contents() []byte {
	return p.data
}
