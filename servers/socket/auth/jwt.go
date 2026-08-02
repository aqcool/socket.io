package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math/big"
	"strings"
	"time"

	socket "github.com/aqcool/socket.io/servers/socket/v3"
)

var (
	ErrMalformedToken   = errors.New("socket.io auth: malformed JWT")
	ErrUnsupportedJWT   = errors.New("socket.io auth: unsupported JWT algorithm")
	ErrInvalidSignature = errors.New("socket.io auth: invalid JWT signature")
	ErrExpiredToken     = errors.New("socket.io auth: expired JWT")
	ErrTokenNotActive   = errors.New("socket.io auth: JWT is not active")
	ErrInvalidIssuer    = errors.New("socket.io auth: invalid JWT issuer")
	ErrInvalidAudience  = errors.New("socket.io auth: invalid JWT audience")
	ErrKeyNotFound      = errors.New("socket.io auth: JWK not found")
)

type Claims map[string]any

type KeySource interface {
	Key(context.Context, string, string) (any, error)
}

type TokenExtractor func(*socket.Socket) (string, error)

type JWTOptions struct {
	Keys       KeySource
	Algorithms []string
	Issuer     string
	Audience   string
	Leeway     time.Duration
	Extractor  TokenExtractor
	Now        func() time.Time
	OnVerified func(*socket.Socket, Claims) error
}

// JWT returns Namespace middleware which validates a compact signed JWT.
func JWT(options *JWTOptions) (socket.NamespaceMiddleware, error) {
	if options == nil || options.Keys == nil || len(options.Algorithms) == 0 {
		return nil, ErrInvalidOptions
	}
	extractor := options.Extractor
	if extractor == nil {
		extractor = DefaultTokenExtractor
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	allowed := make(map[string]struct{}, len(options.Algorithms))
	for _, algorithm := range options.Algorithms {
		allowed[algorithm] = struct{}{}
	}
	return func(s *socket.Socket, next func(*socket.ExtendedError)) {
		token, err := extractor(s)
		var claims Claims
		if err == nil {
			claims, err = verifyJWT(context.Background(), token, options.Keys, allowed)
		}
		if err == nil {
			err = validateClaims(claims, options, now())
		}
		if err == nil && options.OnVerified != nil {
			err = options.OnVerified(s, claims)
		}
		nextAuth(err, next)
	}, nil
}

// DefaultTokenExtractor reads auth.token, then an Authorization Bearer header.
func DefaultTokenExtractor(s *socket.Socket) (string, error) {
	if token, ok := s.Handshake().Auth["token"].(string); ok && token != "" {
		return token, nil
	}
	for name, value := range s.Handshake().Headers {
		if !strings.EqualFold(name, "Authorization") {
			continue
		}
		authorization := firstHeader(value)
		if len(authorization) > 7 && strings.EqualFold(authorization[:7], "Bearer ") {
			return strings.TrimSpace(authorization[7:]), nil
		}
	}
	return "", ErrMissingCredential
}

func verifyJWT(
	ctx context.Context,
	token string,
	keys KeySource,
	allowed map[string]struct{},
) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, ErrMalformedToken
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, ErrMalformedToken
	}
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	if json.Unmarshal(headerBytes, &header) != nil || header.Algorithm == "" || header.Algorithm == "none" {
		return nil, ErrMalformedToken
	}
	if len(allowed) > 0 {
		if _, ok := allowed[header.Algorithm]; !ok {
			return nil, ErrUnsupportedJWT
		}
	}
	key, err := keys.Key(ctx, header.KeyID, header.Algorithm)
	if err != nil {
		return nil, err
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, ErrMalformedToken
	}
	if err = verifySignature(header.Algorithm, key, []byte(parts[0]+"."+parts[1]), signature); err != nil {
		return nil, err
	}
	claimsBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrMalformedToken
	}
	claims := Claims{}
	decoder := json.NewDecoder(strings.NewReader(string(claimsBytes)))
	decoder.UseNumber()
	if decoder.Decode(&claims) != nil {
		return nil, ErrMalformedToken
	}
	return claims, nil
}

func verifySignature(algorithm string, key any, message, signature []byte) error {
	hashFunction, family, err := jwtHash(algorithm)
	if err != nil {
		return err
	}
	hasher := hashFunction.New()
	_, _ = hasher.Write(message)
	digest := hasher.Sum(nil)
	switch family {
	case "HS":
		secret, ok := key.([]byte)
		if !ok {
			return ErrInvalidSignature
		}
		mac := hmac.New(func() hash.Hash { return hashFunction.New() }, secret)
		_, _ = mac.Write(message)
		if !hmac.Equal(signature, mac.Sum(nil)) {
			return ErrInvalidSignature
		}
	case "RS":
		publicKey, ok := key.(*rsa.PublicKey)
		if !ok || rsa.VerifyPKCS1v15(publicKey, hashFunction, digest, signature) != nil {
			return ErrInvalidSignature
		}
	case "ES":
		publicKey, ok := key.(*ecdsa.PublicKey)
		if !ok || len(signature)%2 != 0 {
			return ErrInvalidSignature
		}
		size := len(signature) / 2
		if !ecdsa.Verify(publicKey, digest, new(big.Int).SetBytes(signature[:size]), new(big.Int).SetBytes(signature[size:])) {
			return ErrInvalidSignature
		}
	default:
		return ErrUnsupportedJWT
	}
	return nil
}

func jwtHash(algorithm string) (crypto.Hash, string, error) {
	switch algorithm {
	case "HS256", "RS256", "ES256":
		return crypto.SHA256, algorithm[:2], nil
	case "HS384", "RS384", "ES384":
		return crypto.SHA384, algorithm[:2], nil
	case "HS512", "RS512", "ES512":
		return crypto.SHA512, algorithm[:2], nil
	default:
		return 0, "", ErrUnsupportedJWT
	}
}

func validateClaims(claims Claims, options *JWTOptions, now time.Time) error {
	nowSeconds := float64(now.UnixNano()) / float64(time.Second)
	leeway := options.Leeway.Seconds()
	if exp, ok := numericClaim(claims["exp"]); ok && nowSeconds > exp+leeway {
		return ErrExpiredToken
	}
	if nbf, ok := numericClaim(claims["nbf"]); ok && nowSeconds+leeway < nbf {
		return ErrTokenNotActive
	}
	if options.Issuer != "" {
		issuer, ok := claims["iss"].(string)
		if !ok || issuer != options.Issuer {
			return ErrInvalidIssuer
		}
	}
	if options.Audience != "" && !hasAudience(claims["aud"], options.Audience) {
		return ErrInvalidAudience
	}
	return nil
}

func numericClaim(value any) (float64, bool) {
	switch number := value.(type) {
	case json.Number:
		result, err := number.Float64()
		return result, err == nil
	case float64:
		return number, true
	case int64:
		return float64(number), true
	default:
		return 0, false
	}
}

func hasAudience(value any, expected string) bool {
	switch audience := value.(type) {
	case string:
		return audience == expected
	case []any:
		for _, item := range audience {
			if item == expected {
				return true
			}
		}
	}
	return false
}

// StaticKeySource resolves keys from memory. DefaultKey is used when kid is empty
// or no exact key is present.
type StaticKeySource struct {
	Keys       map[string]any
	DefaultKey any
}

func (s StaticKeySource) Key(_ context.Context, keyID, _ string) (any, error) {
	if key, ok := s.Keys[keyID]; ok {
		return key, nil
	}
	if s.DefaultKey != nil {
		return s.DefaultKey, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, keyID)
}
