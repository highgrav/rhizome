package pgif

const (
	LogLevelDebug         int = 7
	LogLevelInformational int = 6
)

type BackendConfig struct {
	LogLevel      int
	ServerVersion string
}
