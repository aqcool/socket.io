package auth

import (
	"context"
	"crypto/hmac"
	"encoding/base64"
	"encoding/json"
	"hash"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestVerifyJWTAndClaims(t *testing.T) {
	t.Parallel()

	secret := []byte("test-secret-at-least-32-bytes-long")
	now := time.Unix(1_800_000_000, 0)
	token := signedToken(t, "HS256", "primary", Claims{
		"sub": "user-1",
		"iss": "issuer",
		"aud": []string{"api", "socket"},
		"exp": now.Add(time.Minute).Unix(),
	}, secret)
	claims, err := verifyJWT(
		context.Background(),
		token,
		StaticKeySource{Keys: map[string]any{"primary": secret}},
		map[string]struct{}{"HS256": {}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if claims["sub"] != "user-1" {
		t.Fatalf("subject = %v", claims["sub"])
	}
	if err = validateClaims(claims, &JWTOptions{
		Issuer:   "issuer",
		Audience: "socket",
	}, now); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyJWTRejectsSignatureAndClaims(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_000, 0)
	token := signedToken(t, "HS256", "", Claims{
		"exp": now.Add(-time.Minute).Unix(),
	}, []byte("correct-secret"))
	_, err := verifyJWT(
		context.Background(),
		token,
		StaticKeySource{DefaultKey: []byte("wrong-secret")},
		nil,
	)
	if err != ErrInvalidSignature {
		t.Fatalf("signature error = %v", err)
	}
	claims, err := verifyJWT(
		context.Background(),
		token,
		StaticKeySource{DefaultKey: []byte("correct-secret")},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = validateClaims(claims, &JWTOptions{}, now); err != ErrExpiredToken {
		t.Fatalf("claims error = %v", err)
	}
}

func TestRemoteJWKSetCachesAndRefreshes(t *testing.T) {
	t.Parallel()

	var requests atomic.Int64
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		body := `{"keys":[{"kty":"oct","kid":"one","k":"c2VjcmV0"}]}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}
	set, err := NewRemoteJWKSet(JWKSetOptions{
		URL:             "https://issuer.example/.well-known/jwks.json",
		Client:          client,
		RefreshInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := set.Key(context.Background(), "one", "HS256")
	if err != nil {
		t.Fatal(err)
	}
	second, err := set.Key(context.Background(), "one", "HS256")
	if err != nil {
		t.Fatal(err)
	}
	if string(first.([]byte)) != "secret" || string(second.([]byte)) != "secret" {
		t.Fatal("unexpected JWK secret")
	}
	if requests.Load() != 1 {
		t.Fatalf("JWK requests = %d, want 1", requests.Load())
	}
}

func TestMapUserID(t *testing.T) {
	t.Parallel()

	resolve := MapUserID("userId")
	userID, ok := resolve(map[string]any{"userId": "user-1"})
	if !ok || userID != "user-1" {
		t.Fatalf("resolved (%q, %v)", userID, ok)
	}
	if _, ok = resolve(struct{}{}); ok {
		t.Fatal("unexpected match for non-map data")
	}
}

func signedToken(t *testing.T, algorithm, keyID string, claims Claims, secret []byte) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": algorithm, "kid": keyID, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	message := base64.RawURLEncoding.EncodeToString(header) + "." +
		base64.RawURLEncoding.EncodeToString(payload)
	hashFunction, _, err := jwtHash(algorithm)
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(func() hash.Hash { return hashFunction.New() }, secret)
	_, _ = mac.Write([]byte(message))
	return message + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}
