package wire

import (
	"strings"
	"testing"
)

func TestSignIsStableAndSized(t *testing.T) {
	const payload = "node-a|01JCV0000000000000000000|42|1700000000000000000"

	first := Sign(testKey, payload)
	second := Sign(testKey, payload)
	if first != second {
		t.Errorf("Sign is not deterministic: %q vs %q", first, second)
	}
	if len(first) != hmacHexLen {
		t.Errorf("signature length = %d, want %d", len(first), hmacHexLen)
	}
	for _, c := range first {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("signature contains non-hex character %q: %s", c, first)
		}
	}
}

func TestSignSeparatesInputs(t *testing.T) {
	// Different payloads and different keys must not collide in the 48 bits we
	// keep. This is not a security claim, it is a sanity check that we are
	// actually mixing in every field.
	cases := []struct {
		name    string
		key     []byte
		payload string
	}{
		{"baseline", testKey, "a|01JCV0000000000000000000|1|1"},
		{"different seq", testKey, "a|01JCV0000000000000000000|2|1"},
		{"different src", testKey, "b|01JCV0000000000000000000|1|1"},
		{"different epoch", testKey, "a|01JCV0000000000000000001|1|1"},
		{"different ts", testKey, "a|01JCV0000000000000000000|1|2"},
		{"different key", []byte("other-key"), "a|01JCV0000000000000000000|1|1"},
	}

	seen := make(map[string]string, len(cases))
	for _, tc := range cases {
		sig := Sign(tc.key, tc.payload)
		if prior, dup := seen[sig]; dup {
			t.Errorf("%s collided with %s (both %s)", tc.name, prior, sig)
		}
		seen[sig] = tc.name
	}
}

func TestEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b string
		want bool
	}{
		{"identical", "0123456789ab", "0123456789ab", true},
		{"one character differs", "0123456789ab", "0123456789ac", false},
		{"different lengths", "0123456789ab", "0123", false},
		{"both empty", "", "", true},
		{"one empty", "0123456789ab", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Equal(tc.a, tc.b); got != tc.want {
				t.Errorf("Equal(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestKeyFromEnv(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		t.Setenv(KeyEnvVar, "")
		if _, err := KeyFromEnv(); err != ErrNoKey {
			t.Errorf("error = %v, want ErrNoKey", err)
		}
	})

	t.Run("whitespace only", func(t *testing.T) {
		t.Setenv(KeyEnvVar, "   \n\t ")
		if _, err := KeyFromEnv(); err != ErrNoKey {
			t.Errorf("error = %v, want ErrNoKey", err)
		}
	})

	t.Run("present", func(t *testing.T) {
		t.Setenv(KeyEnvVar, "  a-real-key  ")
		key, err := KeyFromEnv()
		if err != nil {
			t.Fatalf("KeyFromEnv: %v", err)
		}
		if string(key) != "a-real-key" {
			t.Errorf("key = %q, want %q", key, "a-real-key")
		}
	})
}

func TestRedact(t *testing.T) {
	// PRD section 8.5: the key never appears in metrics, reports or logs.
	key := []byte("super-secret-key")
	in := "connecting with key super-secret-key to sink es"

	got := Redact(in, key)
	if strings.Contains(got, string(key)) {
		t.Errorf("Redact left the key in place: %q", got)
	}
	if !strings.Contains(got, "[redacted]") {
		t.Errorf("Redact did not insert a marker: %q", got)
	}

	if got := Redact(in, nil); got != in {
		t.Errorf("Redact with no key altered the string: %q", got)
	}
}
