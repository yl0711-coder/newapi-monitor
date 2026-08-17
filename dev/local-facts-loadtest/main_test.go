package main

import (
	"strings"
	"testing"
)

func TestValidateLoopbackBase(t *testing.T) {
	for _, raw := range []string{"http://127.0.0.1:28100", "http://localhost:28101", "http://[::1]:28100"} {
		if err := validateLoopbackBase(raw); err != nil {
			t.Fatalf("loopback rejected %q: %v", raw, err)
		}
	}
	for _, raw := range []string{"https://127.0.0.1:28100", "http://10.0.0.1:28100", "http://example.com", "http://127.0.0.1:28100/path"} {
		if err := validateLoopbackBase(raw); err == nil {
			t.Fatalf("unsafe base accepted: %q", raw)
		}
	}
}

func TestSignAdminSessionShape(t *testing.T) {
	token := signAdminSession("secret", "director", 100, 123)
	if strings.Count(token, ".") != 1 || len(token) < 64 {
		t.Fatalf("invalid session shape: %q", token)
	}
}
