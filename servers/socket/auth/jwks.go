package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type JWKSetOptions struct {
	URL             string
	Client          *http.Client
	RefreshInterval time.Duration
}

// RemoteJWKSet caches keys from an HTTPS JWK Set endpoint and refreshes them
// on expiry or when a requested kid is not in the current set.
type RemoteJWKSet struct {
	options   JWKSetOptions
	mu        sync.Mutex
	keys      map[string]any
	expiresAt time.Time
}

func NewRemoteJWKSet(options JWKSetOptions) (*RemoteJWKSet, error) {
	endpoint, err := url.Parse(options.URL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" {
		return nil, ErrInvalidOptions
	}
	if options.Client == nil {
		options.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if options.RefreshInterval <= 0 {
		options.RefreshInterval = 5 * time.Minute
	}
	return &RemoteJWKSet{options: options, keys: make(map[string]any)}, nil
}

func (r *RemoteJWKSet) Key(ctx context.Context, keyID, _ string) (any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Now().Before(r.expiresAt) {
		if key, ok := r.keys[keyID]; ok {
			return key, nil
		}
		if keyID == "" && len(r.keys) == 1 {
			return onlyKey(r.keys), nil
		}
	}
	if err := r.refresh(ctx); err != nil {
		return nil, err
	}
	if key, ok := r.keys[keyID]; ok {
		return key, nil
	}
	if keyID == "" && len(r.keys) == 1 {
		return onlyKey(r.keys), nil
	}
	return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, keyID)
}

func onlyKey(keys map[string]any) any {
	for _, key := range keys {
		return key
	}
	return nil
}

func (r *RemoteJWKSet) refresh(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.options.URL, nil)
	if err != nil {
		return err
	}
	response, err := r.options.Client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("socket.io auth: JWK endpoint returned %s", response.Status)
	}
	var set struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err = json.NewDecoder(response.Body).Decode(&set); err != nil {
		return err
	}
	keys := make(map[string]any, len(set.Keys))
	for _, raw := range set.Keys {
		keyID, key, parseErr := parseJWK(raw)
		if parseErr != nil {
			return parseErr
		}
		keys[keyID] = key
	}
	r.keys = keys
	r.expiresAt = time.Now().Add(r.options.RefreshInterval)
	return nil
}

func parseJWK(raw json.RawMessage) (string, any, error) {
	var key struct {
		KeyID string `json:"kid"`
		Type  string `json:"kty"`
		Curve string `json:"crv"`
		N     string `json:"n"`
		E     string `json:"e"`
		X     string `json:"x"`
		Y     string `json:"y"`
		K     string `json:"k"`
	}
	if err := json.Unmarshal(raw, &key); err != nil {
		return "", nil, err
	}
	switch key.Type {
	case "RSA":
		n, err := decodeBigInt(key.N)
		if err != nil {
			return "", nil, err
		}
		e, err := decodeBigInt(key.E)
		if err != nil {
			return "", nil, err
		}
		if !e.IsInt64() || e.Int64() <= 1 || int64(int(e.Int64())) != e.Int64() {
			return "", nil, ErrInvalidCredential
		}
		return key.KeyID, &rsa.PublicKey{N: n, E: int(e.Int64())}, nil
	case "EC":
		curve := map[string]elliptic.Curve{
			"P-256": elliptic.P256(),
			"P-384": elliptic.P384(),
			"P-521": elliptic.P521(),
		}[key.Curve]
		if curve == nil {
			return "", nil, ErrUnsupportedJWT
		}
		x, err := decodeBigInt(key.X)
		if err != nil {
			return "", nil, err
		}
		y, err := decodeBigInt(key.Y)
		if err != nil {
			return "", nil, err
		}
		return key.KeyID, &ecdsa.PublicKey{Curve: curve, X: x, Y: y}, nil
	case "oct":
		secret, err := base64.RawURLEncoding.DecodeString(key.K)
		return key.KeyID, secret, err
	default:
		return "", nil, ErrUnsupportedJWT
	}
}

func decodeBigInt(value string) (*big.Int, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(decoded), nil
}
