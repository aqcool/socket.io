package engine

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
)

var randomStringFallback atomic.Uint64

func randomString() string {
	var value [6]byte
	if _, err := rand.Read(value[:]); err == nil {
		// Six bytes encode to exactly eight URL-safe characters without
		// padding, matching engine.io-client's public invariant.
		return base64.RawURLEncoding.EncodeToString(value[:])
	}
	return fmt.Sprintf("%08x", randomStringFallback.Add(1)&0xffffffff)
}

func notifyNetwork(options SocketOptionsInterface, event *NetworkEvent) {
	if options == nil {
		return
	}
	if proxy := options.ProxyURL(); proxy != nil {
		event.Proxy = proxy.Redacted()
	}
	if event.Err != nil {
		event.Kind = classifyNetworkError(event.Err, event.Proxy != "")
	}
	if observer := options.NetworkObserver(); observer != nil {
		observer(*event)
	}
}

func classifyNetworkError(err error, hasProxy bool) string {
	var recordHeaderError tls.RecordHeaderError
	var certificateInvalid x509.CertificateInvalidError
	var unknownAuthority x509.UnknownAuthorityError
	if errors.As(err, &recordHeaderError) || errors.As(err, &certificateInvalid) || errors.As(err, &unknownAuthority) {
		return "tls"
	}
	if hasProxy {
		return "proxy"
	}
	var networkError *net.OpError
	if errors.As(err, &networkError) {
		return "dial"
	}
	return "transport"
}
