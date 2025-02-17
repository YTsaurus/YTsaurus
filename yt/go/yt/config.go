package yt

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/nop"
	zaplog "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/guid"
)

type Config struct {
	// Proxy configures address of YT HTTP proxy in form: name|[scheme://]fqdn[:port].
	//
	// If Proxy is not set, value of YT_PROXY environment variable is used instead.
	//
	// Might be equal to cluster name. E.g. hahn or markov.
	// Might be equal to hostname with optional scheme and port.
	// E.g. localhost:12345 or https://hahn.yt.yandex.net.
	Proxy string

	// RPCProxy pins address of YT RPC proxy.
	//
	// If set, proxy discovery is disabled and provided value is used for all requests.
	//
	// If left empty, RPC proxies are discovered via HTTP using Proxy setting.
	//
	// Only relevant for RPC client.
	RPCProxy string

	// ProxyRole configures desired proxy role used by the client.
	//
	// If not set, default role is used.
	ProxyRole string

	// NetworkName configures network name used by RPC proxy.
	//
	// Only relevant for RPC client.
	NetworkName string

	// HostsPath specifies the path used to discover HTTP proxies.
	//
	// If not set, defaults to "hosts".
	HostsPath string

	// UseTLS enables TLS for all connections to cluster.
	//
	// By default, client will not use TLS.
	//
	// TLS is not supported in local mode.
	UseTLS bool

	// CertificateAuthorityData contains PEM-encoded certificate authority certificates.
	//
	// Client appends this certificates to the system cert pool.
	//
	// This option is relevant for HTTP client with enabled TLS.
	CertificateAuthorityData []byte

	// PeerAlternativeHostName overrides server name for bus TLS verification.
	PeerAlternativeHostName string

	// Token configures OAuth token used by the client.
	//
	// If Token is not set, value of YT_TOKEN environment variable is used instead.
	Token string

	// ReadTokenFromFile
	//
	// When this variable is set, client tries reading token from ~/.yt/token file.
	ReadTokenFromFile bool

	// Credentials can be used for authentication.
	//
	// If Credentials are not set, OAuth token is used.
	Credentials Credentials

	// TVMFn is used to issue service tickets for YT API requests.
	//
	// TVM is a preferred way of service authentication.
	//
	// If TVMFn is not set, Credentials or OAuth token are used.
	//
	// Assign yttvm.TVMFn(tvm.Client) to this field, if you wish to enable tvm authentication.
	TVMFn TVMFn

	// UseTVMOnlyEndpoint configures client to use tvm-only endpoints in cluster connection.
	UseTVMOnlyEndpoint bool

	// DisableProxyDiscovery configures whether proxy discovery is enabled.
	//
	// Typically proxy discovery should be enabled, but in case if there is no
	// network connectivity from the client to the proxy instances and balancer
	// is used instead, proxy discovery should be disabled.
	DisableProxyDiscovery bool

	// Logger overrides default logger, used by the client.
	//
	// When Logger is not set, logging behaviour is configured by YT_LOG_LEVEL environment variable.
	//
	// If YT_LOG_LEVEL is not set, no logging is performed. Otherwise logs are written to stderr,
	// with log level derived from value of YT_LOG_LEVEL variable.
	//
	// WARNING: Running YT client in production without debug logs is highly discouraged.
	Logger log.Structured

	// Tracer overrides default tracer, used by the client
	//
	// When Tracer is not set opentracing.GlobalTracer is used.
	//
	// If opentracing.GlobalTracer is not set, no tracing is performed.
	Tracer opentracing.Tracer

	// TraceFn extracts trace parent from request context.
	//
	// This function is extracted into config in order to avoid direct dependency on jaeger client.
	//
	// Assign ytjaeger.TraceFn to this field, if you wish to enable tracing.
	TraceFn TraceFn

	// LightRequestTimeout specifies default timeout for light requests. Timeout includes all retries and backoffs.
	// Timeout for single request is not configurable right now.
	//
	// A Timeout of zero means no timeout (retries never stop).
	// Client can still specify timeout on per-request basis using context.
	//
	// nil value means default timeout of 5 minutes.
	LightRequestTimeout *time.Duration

	// TxTimeout specifies timeout of YT transaction (both master and tablet).
	//
	// YT transaction is aborted by server after not receiving pings from client for TxTimeout seconds.
	//
	// TxTimeout of zero means default timeout of 15 seconds.
	TxTimeout time.Duration

	// TxPingPeriod specifies period of pings for YT transactions.
	//
	// TxPingPeriod of zero means default value of 3 seconds.
	TxPingPeriod time.Duration

	// AllowRequestsFromJob explicitly allows creating client inside YT job.
	//
	// WARNING: This option can be enabled ONLY after explicit approval from YT team. If you enable this option
	// without approval, your might be BANNED.
	//
	// If you need to read tables, or access cypress from YT job, use API provided by mapreduce package, or
	// redesign your application.
	//
	// Typical mapreduce operation can launch hundred of thousands concurrent jobs. If each job makes even a single request,
	// that could easily lead to master/proxy overload.
	AllowRequestsFromJob bool

	// CompressionCodec specifies codec used for compression of client requests and server responses.
	//
	// NOTE: this codec has nothing to do with codec used for storing table chunks.
	CompressionCodec ClientCompressionCodec
}

func (c *Config) GetProxy() (string, error) {
	if c.Proxy != "" {
		return c.Proxy, nil
	}

	if proxy := os.Getenv("YT_PROXY"); proxy != "" {
		return proxy, nil
	}

	return "", xerrors.New("YT proxy is not set (either Config.Proxy or YT_PROXY must be set)")
}

func (c *Config) GetCusterURL() (ClusterURL, error) {
	proxy, err := c.GetProxy()
	if err != nil {
		return ClusterURL{}, err
	}

	if !strings.Contains(proxy, ":") {
		if !strings.Contains(proxy, ".") && !strings.Contains(proxy, "localhost") {
			const suffix = ".yt.yandex.net"
			proxy += suffix
		}
	}

	if !strings.Contains(proxy, "//") {
		if c.UseTLS {
			proxy = "https://" + proxy
		} else {
			proxy = "http://" + proxy
		}
	}

	parsedURL, err := url.Parse(proxy)
	if err != nil {
		return ClusterURL{}, err
	}

	if c.UseTVMOnlyEndpoint && !strings.Contains(parsedURL.Host, ":") {
		port := TVMOnlyHTTPProxyPort
		if c.UseTLS || parsedURL.Scheme == "https" {
			port = TVMOnlyHTTPSProxyPort
		}

		parsedURL.Host = fmt.Sprintf("tvm.%v:%v", parsedURL.Host, port)
	}

	return ClusterURL{
		Scheme:           parsedURL.Scheme,
		Address:          parsedURL.Host,
		DisableDiscovery: c.DisableProxyDiscovery,
	}, nil
}

func (c *Config) getTokenFileFromEnv() (string, error) {
	if tokenFile := os.Getenv("YT_TOKEN_FILE"); tokenFile != "" {
		if strings.HasPrefix(tokenFile, "~/") {
			u, err := user.Current()
			if err != nil {
				return "", err
			}
			return filepath.Join(u.HomeDir, tokenFile[2:]), nil
		}
		return tokenFile, nil
	}
	return "", nil
}

func (c *Config) getPathToTokenFile() (string, error) {
	tokenFile, err := c.getTokenFileFromEnv()
	if err != nil {
		return "", err
	}

	if tokenFile != "" {
		return tokenFile, nil
	}

	u, err := user.Current()
	if err != nil {
		return "", err
	}
	return filepath.Join(u.HomeDir, ".yt", "token"), nil
}

func (c *Config) GetToken() string {
	if c.Token != "" {
		return c.Token
	}

	if token := os.Getenv("YT_TOKEN"); token != "" {
		return token
	}

	if c.ReadTokenFromFile {
		tokenFile, err := c.getPathToTokenFile()
		if err != nil {
			return ""
		}

		token, err := os.ReadFile(tokenFile)
		if err != nil {
			return ""
		}

		return strings.Trim(string(token), "\n")
	}

	return ""
}

func (c *Config) GetLogger() log.Structured {
	if c.Logger != nil {
		return c.Logger
	}

	logLevel := os.Getenv("YT_LOG_LEVEL")
	if logLevel == "" {
		return (&nop.Logger{}).Structured()
	}

	lvl, err := log.ParseLevel(logLevel)
	if err != nil {
		lvl = log.DebugLevel
	}

	config := zaplog.ConsoleConfig(lvl)
	config.OutputPaths = []string{"stderr"}

	l, err := zaplog.New(config)
	if err != nil {
		panic(fmt.Sprintf("failed to configure default logger: %+v", err))
	}
	return l.Structured()
}

func (c *Config) GetTracer() opentracing.Tracer {
	if c.Tracer != nil {
		return c.Tracer
	}

	return opentracing.GlobalTracer()
}

func (c *Config) GetLightRequestTimeout() time.Duration {
	if c.LightRequestTimeout != nil {
		return *c.LightRequestTimeout
	}

	return DefaultLightRequestTimeout
}

func (c *Config) GetTxTimeout() time.Duration {
	if c.TxTimeout == 0 {
		return DefaultTxTimeout
	}

	return c.TxTimeout
}

func (c *Config) GetTxPingPeriod() time.Duration {
	if c.TxPingPeriod == 0 {
		return DefaultTxPingPeriod
	}

	return c.TxPingPeriod
}

func (c *Config) GetClientCompressionCodec() ClientCompressionCodec {
	if c.CompressionCodec == ClientCodecDefault {
		return ClientCodecZSTDDefault
	}

	return c.CompressionCodec
}

const (
	TVMOnlyHTTPProxyPort  = 9026
	TVMOnlyHTTPSProxyPort = 9443
	TVMOnlyRPCProxyPort   = 9027
)

type ClusterURL struct {
	Scheme           string
	Address          string
	DisableDiscovery bool
}

const (
	DefaultLightRequestTimeout = 5 * time.Minute
	DefaultTxTimeout           = 15 * time.Second
	DefaultTxPingPeriod        = 3 * time.Second
)

// ClientCompressionCodec. See yt.Config doc for more details.
type ClientCompressionCodec int

const (
	// Default compression codec, selected by YT team. Particular choice may change in the future.
	ClientCodecDefault ClientCompressionCodec = iota

	// Use default GZIP codec, provided by net/http.
	ClientCodecGZIP

	// No compression at all. It almost never makes sense to disable compression in production.
	ClientCodecNone

	ClientCodecSnappy

	ClientCodecZSTDFastest
	ClientCodecZSTDDefault
	ClientCodecZSTDBetterCompression

	ClientCodecBrotliFastest
	ClientCodecBrotliDefault
	// ClientCodecBrotliBestCompression
)

func (c ClientCompressionCodec) BlockCodec() (string, bool) {
	switch c {
	case ClientCodecSnappy:
		return "snappy", true
	case ClientCodecZSTDFastest:
		return "zstd_1", true
	case ClientCodecZSTDDefault:
		return "zstd_3", true
	case ClientCodecZSTDBetterCompression:
		return "zstd_7", true
	case ClientCodecBrotliFastest:
		return "brotli_1", true
	case ClientCodecBrotliDefault:
		return "brotli_6", true
		//	case ClientCodecBrotliBestCompression:
		//		return "brotli_11", true
	default:
		return "", false
	}
}

type TVMFn func(ctx context.Context) (string, error)

type TraceFn func(ctx context.Context) (traceID guid.GUID, spanID uint64, flags byte, ok bool)
