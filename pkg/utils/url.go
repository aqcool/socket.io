package utils

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

var (
	ErrEmptyURI          = errors.New("URI must not be empty")
	ErrUnsupportedScheme = errors.New("unsupported URI scheme")
)

type ParsedUrl struct {
	*url.URL

	Hostname string
	Port     string
	Id       string
}

func Url(uri string, path string) (parsedUrl *ParsedUrl, err error) {
	if uri == "" {
		return nil, ErrEmptyURI
	}
	// Match the official Node client when no browser location is available:
	// protocol-relative and bare host URLs default to HTTPS. A single-leading-
	// slash relative path still requires an explicit base URL in Go.
	if strings.HasPrefix(uri, "//") {
		uri = "https:" + uri
	} else if !strings.HasPrefix(uri, "/") && !strings.Contains(uri, "://") {
		uri = "https://" + uri
	}

	url, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	switch url.Scheme {
	case "http", "https", "ws", "wss":
		// valid schemes
	case "":
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedScheme, url.Scheme)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedScheme, url.Scheme)
	}

	parsedUrl = &ParsedUrl{URL: url}
	parsedUrl.Hostname = parsedUrl.URL.Hostname()
	parsedUrl.Port = parsedUrl.URL.Port()

	if parsedUrl.Port == "" {
		switch parsedUrl.Scheme {
		case "http", "ws":
			parsedUrl.Port = "80"
		case "https", "wss":
			parsedUrl.Port = "443"
		}
	}

	if parsedUrl.Path == "" {
		parsedUrl.Path = "/"
	}

	parsedUrl.Id = fmt.Sprintf("%s://%s%s", parsedUrl.Scheme, net.JoinHostPort(parsedUrl.Hostname, parsedUrl.Port), path)

	return parsedUrl, nil
}
