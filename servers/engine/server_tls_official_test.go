package engine

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v3/config"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/gorilla/websocket"
)

func TestOfficialServerTLSClientAuthentication(t *testing.T) {
	serverCertificate, clientCertificate, roots := makeOfficialTLSCertificates(t)
	for _, test := range []struct {
		name              string
		transport         string
		requireClientCert bool
		provideClientCert bool
	}{
		{name: "key and certificate polling", transport: "polling", requireClientCert: true, provideClientCert: true},
		{name: "CA without required authentication polling", transport: "polling"},
		{name: "key and certificate websocket", transport: "websocket", requireClientCert: true, provideClientCert: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetAllowUpgrades(false)
			server := NewServer(options)
			message := make(chan string, 1)
			_ = server.On("connection", func(args ...any) {
				_ = args[0].(Socket).Once("message", func(messageArgs ...any) {
					message <- messageArgs[0].(types.BufferInterface).String()
				})
			})

			tlsConfig := &tls.Config{
				Certificates: []tls.Certificate{serverCertificate},
				ClientCAs:    roots,
				MinVersion:   tls.VersionTLS12,
			}
			if test.requireClientCert {
				tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
			} else {
				// Node's requestCert:true/rejectUnauthorized:false equivalent:
				// ask for a certificate but allow the client to omit it.
				tlsConfig.ClientAuth = tls.RequestClientCert
			}
			httpServer := httptest.NewUnstartedServer(server)
			httpServer.TLS = tlsConfig
			httpServer.StartTLS()
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			clientTLS := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
			if test.provideClientCert {
				clientTLS.Certificates = []tls.Certificate{clientCertificate}
			}
			if test.transport == "polling" {
				client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLS}}
				response, err := client.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling")
				if err != nil {
					t.Fatalf("TLS Polling handshake: %v", err)
				}
				body, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if readErr != nil || response.StatusCode != http.StatusOK {
					t.Fatalf("TLS Polling handshake = %d/%q, error=%v", response.StatusCode, body, readErr)
				}
				var opened struct {
					SID string `json:"sid"`
				}
				if decodeErr := json.Unmarshal([]byte(strings.TrimPrefix(string(body), "0")), &opened); decodeErr != nil || opened.SID == "" {
					t.Fatalf("decoding TLS Polling OPEN %q: %v", body, decodeErr)
				}
				sid := opened.SID
				request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, httpServer.URL+"/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid), strings.NewReader("4hello"))
				if err != nil {
					t.Fatalf("creating TLS Polling POST: %v", err)
				}
				request.Header.Set("Content-Type", "text/plain;charset=UTF-8")
				postResponse, err := client.Do(request)
				if err != nil {
					t.Fatalf("TLS Polling POST: %v", err)
				}
				_, _ = io.Copy(io.Discard, postResponse.Body)
				_ = postResponse.Body.Close()
				if postResponse.StatusCode != http.StatusOK {
					t.Fatalf("TLS Polling POST status = %d", postResponse.StatusCode)
				}
			} else {
				dialer := &websocket.Dialer{TLSClientConfig: clientTLS}
				wsURL := "wss" + strings.TrimPrefix(httpServer.URL, "https") + "/engine.io/?EIO=4&transport=websocket"
				connection, _, err := dialer.Dial(wsURL, nil)
				if err != nil {
					t.Fatalf("TLS WebSocket handshake: %v", err)
				}
				t.Cleanup(func() { _ = connection.Close() })
				if _, _, err := connection.ReadMessage(); err != nil {
					t.Fatalf("reading TLS WebSocket OPEN: %v", err)
				}
				if err := connection.WriteMessage(websocket.TextMessage, []byte("4hello")); err != nil {
					t.Fatalf("writing TLS WebSocket message: %v", err)
				}
			}

			select {
			case got := <-message:
				if got != "hello" {
					t.Fatalf("TLS message = %q, want hello", got)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("TLS message was not delivered")
			}
		})
	}
}

func makeOfficialTLSCertificates(t *testing.T) (tls.Certificate, tls.Certificate, *x509.CertPool) {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Engine.IO test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating CA certificate: %v", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}

	issue := func(serial int64, commonName string, usages []x509.ExtKeyUsage, server bool) tls.Certificate {
		key, keyErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if keyErr != nil {
			t.Fatalf("generating %s key: %v", commonName, keyErr)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: commonName},
			NotBefore:    now.Add(-time.Minute),
			NotAfter:     now.Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  usages,
		}
		if server {
			template.DNSNames = []string{"localhost"}
			template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		}
		certificateDER, certificateErr := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if certificateErr != nil {
			t.Fatalf("creating %s certificate: %v", commonName, certificateErr)
		}
		keyDER, keyErr := x509.MarshalPKCS8PrivateKey(key)
		if keyErr != nil {
			t.Fatalf("marshaling %s key: %v", commonName, keyErr)
		}
		certificate, loadErr := tls.X509KeyPair(
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER}),
			pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		)
		if loadErr != nil {
			t.Fatalf("loading %s key pair: %v", commonName, loadErr)
		}
		return certificate
	}

	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return issue(2, "localhost", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, true),
		issue(3, "Engine.IO test client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, false), roots
}
