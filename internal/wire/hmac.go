package wire

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"os"
	"strings"
)

// ErrNoKey reports that no HMAC key was configured. LossLedger refuses to run
// unsigned: an unsigned ledger cannot tell our canary lines from any other line
// that happens to look like one, and a ledger that can be polluted is worthless.
var ErrNoKey = errors.New("wire: no HMAC key configured")

// KeyEnvVar is the environment variable the emitter and verifier read the
// shared key from. It is an environment variable rather than a config field so
// that the key stays out of the config files people paste into issues.
const KeyEnvVar = "LOSSLEDGER_HMAC_KEY"

// Sign returns the first 12 hex characters of HMAC-SHA256(key, payload).
func Sign(key []byte, payload string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(payload))
	full := hex.EncodeToString(mac.Sum(nil))
	return full[:hmacHexLen]
}

// Equal compares two signatures in constant time.
func Equal(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// KeyFromEnv reads the shared key from the environment.
//
// The key never appears in metrics, reports or logs (PRD section 8.5), so this
// is the only place it enters the process, and callers are expected to hold it
// in memory and nowhere else.
func KeyFromEnv() ([]byte, error) {
	v := strings.TrimSpace(os.Getenv(KeyEnvVar))
	if v == "" {
		return nil, ErrNoKey
	}
	return []byte(v), nil
}

// Redact replaces anything that looks like the key with a fixed marker. Call it
// on any string headed for a log line, a metric label or a report.
func Redact(s string, key []byte) string {
	if len(key) == 0 {
		return s
	}
	return strings.ReplaceAll(s, string(key), "[redacted]")
}
