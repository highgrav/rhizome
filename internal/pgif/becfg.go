package pgif

type FnAuthorizeUser func(string, string) (bool, error)

type BackendConfig struct {
	LogLevel        int
	ServerVersion   string
	FnAuthorizeUser FnAuthorizeUser
}
