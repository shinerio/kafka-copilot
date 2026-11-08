//go:build windows

package config

import (
	"encoding/base64"
	"fmt"
	"syscall"
	"unsafe"
)

var (
	crypt32          = syscall.NewLazyDLL("crypt32.dll")
	procCryptProtect = crypt32.NewProc("CryptProtectData")
	procCryptUnprotect = crypt32.NewProc("CryptUnprotectData")
)

type dataBlob struct {
	cbData uint32
	pbData *byte
}

// Encrypt encrypts plaintext using Windows DPAPI.
func Encrypt(plaintext string) (string, error) {
	return dpapiProtect(plaintext)
}

// Decrypt decrypts ciphertext using Windows DPAPI.
func Decrypt(ciphertext string) (string, error) {
	return dpapiUnprotect(ciphertext)
}

func dpapiProtect(plaintext string) (string, error) {
	in := []byte(plaintext)
	inBlob := dataBlob{cbData: uint32(len(in)), pbData: &in[0]}
	var outBlob dataBlob
	r, _, err := procCryptProtect.Call(
		uintptr(unsafe.Pointer(&inBlob)),
		0, // optEntropy
		0, // reserved
		0, // pvDesc
		0, // prompt
		0, // flags
		uintptr(unsafe.Pointer(&outBlob)),
	)
	if r == 0 {
		return "", fmt.Errorf("CryptProtectData failed: %w", err)
	}
	defer localFree(outBlob)
	out := make([]byte, outBlob.cbData)
	copy(out, (*[1 << 30]byte)(unsafe.Pointer(outBlob.pbData))[:outBlob.cbData])
	return base64.StdEncoding.EncodeToString(out), nil
}

func dpapiUnprotect(ciphertext string) (string, error) {
	in, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}
	inBlob := dataBlob{cbData: uint32(len(in)), pbData: &in[0]}
	var outBlob dataBlob
	r, _, err := procCryptUnprotect.Call(
		uintptr(unsafe.Pointer(&inBlob)),
		0,
		0,
		0,
		0,
		0,
		uintptr(unsafe.Pointer(&outBlob)),
	)
	if r == 0 {
		return "", fmt.Errorf("CryptUnprotectData failed: %w", err)
	}
	defer localFree(outBlob)
	out := make([]byte, outBlob.cbData)
	copy(out, (*[1 << 30]byte)(unsafe.Pointer(outBlob.pbData))[:outBlob.cbData])
	return string(out), nil
}

var (
	kernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLocalFree = kernel32.NewProc("LocalFree")
)

func localFree(blob dataBlob) {
	procLocalFree.Call(uintptr(unsafe.Pointer(blob.pbData)))
}
