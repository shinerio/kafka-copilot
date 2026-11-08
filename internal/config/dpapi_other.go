//go:build !windows

package config

import (
	"encoding/base64"
	"fmt"
)

// Encrypt encrypts plaintext using base64 (non-Windows fallback for dev).
func Encrypt(plaintext string) (string, error) {
	return base64.StdEncoding.EncodeToString([]byte(plaintext)), nil
}

// Decrypt decrypts base64-encoded ciphertext (non-Windows fallback).
func Decrypt(ciphertext string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}
	return string(data), nil
}
