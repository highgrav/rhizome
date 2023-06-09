package pgif

import "crypto/tls"

type FnAuthorizeUser func(string, string) (bool, error)

type BackendConfig struct {
	ServerName      string
	LogLevel        int
	ServerVersion   string
	FnAuthorizeUser FnAuthorizeUser
	UseTLS          bool
	TLSCertDir      string
	TLSCertName     string
	TLSKeyName      string
	TLS             *tls.Config
}
