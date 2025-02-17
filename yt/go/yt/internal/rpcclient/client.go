package rpcclient

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal"
)

var _ yt.Client = (*client)(nil)

type client struct {
	Encoder

	conf        *yt.Config
	clusterURL  yt.ClusterURL
	credentials yt.Credentials

	log    log.Structured
	tracer opentracing.Tracer

	// httpClient is used to retrieve available proxies.
	httpClient *http.Client
	proxySet   *internal.ProxySet

	connPool *ConnPool
	stop     *internal.StopGroup
}

func NewClient(conf *yt.Config) (*client, error) {
	clusterURL, err := conf.GetCusterURL()
	if err != nil {
		return nil, err
	}

	certPool, err := internal.NewCertPool()
	if err != nil {
		return nil, err
	}

	c := &client{
		conf:       conf,
		clusterURL: clusterURL,
		log:        conf.GetLogger(),
		tracer:     conf.GetTracer(),
		stop:       internal.NewStopGroup(),
	}

	tlsConfig := tls.Config{
		RootCAs: certPool,
	}

	c.httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        0,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     30 * time.Second,

			TLSHandshakeTimeout: 10 * time.Second,
			TLSClientConfig:     &tlsConfig,
		},
		Timeout: 60 * time.Second,
	}

	if conf.Credentials != nil {
		c.credentials = conf.Credentials
	} else if token := conf.GetToken(); token != "" {
		c.credentials = &yt.TokenCredentials{Token: token}
	}

	c.proxySet = &internal.ProxySet{UpdateFn: c.listRPCProxies}

	c.connPool = NewConnPool(func(ctx context.Context, addr string) BusConn {
		clientOpts := []bus.ClientOption{
			bus.WithLogger(c.log.Logger()),
			bus.WithDefaultProtocolVersionMajor(ProtocolVersionMajor),
		}
		if conf.UseTLS {
			busTLSConfig := tlsConfig.Clone()
			if conf.PeerAlternativeHostName != "" {
				// TODO(khlebnikov) use custom VerifyPeerCertificate.
				busTLSConfig.ServerName = conf.PeerAlternativeHostName
			} else {
				busTLSConfig.ServerName = addr
			}
			clientOpts = append(clientOpts, bus.WithEncryptionMode(bus.EncryptionModeRequired))
			clientOpts = append(clientOpts, bus.WithTLSConfig(busTLSConfig))
		}
		return bus.NewClient(ctx, addr, clientOpts...)
	}, c.log)

	c.Encoder.StartCall = c.startCall
	c.Encoder.Invoke = c.invoke
	c.Encoder.InvokeInTx = c.invokeInTx
	c.Encoder.InvokeReadRow = c.doReadRow

	proxyBouncer := &ProxyBouncer{Log: c.log, ProxySet: c.proxySet, ConnPool: c.connPool}
	requestLogger := &LoggingInterceptor{Structured: c.log}
	requestTracer := &TracingInterceptor{Tracer: c.tracer}
	mutationRetrier := &MutationRetrier{Log: c.log}
	readRetrier := &Retrier{RequestTimeout: conf.GetLightRequestTimeout(), Log: c.log}
	errorWrapper := &ErrorWrapper{}

	c.Encoder.Invoke = c.Encoder.Invoke.
		Wrap(proxyBouncer.Intercept).
		Wrap(requestLogger.Intercept).
		Wrap(requestTracer.Intercept).
		Wrap(mutationRetrier.Intercept).
		Wrap(readRetrier.Intercept).
		Wrap(errorWrapper.Intercept)

	c.Encoder.InvokeInTx = c.Encoder.InvokeInTx.
		Wrap(readRetrier.InterceptInTx)

	return c, nil
}

func (c *client) doReadRow(
	ctx context.Context,
	call *Call,
	rsp ProtoRowset,
) (yt.TableReader, error) {
	var rspAttachments [][]byte

	err := c.Invoke(ctx, call, rsp, bus.WithResponseAttachments(&rspAttachments))
	if err != nil {
		return nil, err
	}

	rows, err := decodeFromWire(rspAttachments)
	if err != nil {
		err := xerrors.Errorf("unable to decode response from wire format: %w", err)
		return nil, err
	}

	return newTableReader(rows, rsp.GetRowsetDescriptor())
}

func (c *client) invoke(
	ctx context.Context,
	call *Call,
	rsp proto.Message,
	opts ...bus.SendOption,
) error {
	addr := call.RequestedProxy
	if addr == "" {
		var err error
		addr, err = c.pickRPCProxy(ctx)
		if err != nil {
			return err
		}
	}
	call.SelectedProxy = addr

	opts = append(opts,
		bus.WithRequestID(call.CallID),
	)

	credentials, err := c.requestCredentials(ctx)
	if err != nil {
		return err
	}
	if credentials != nil {
		opts = append(opts, bus.WithCredentials(credentials))
	}

	if call.Attachments != nil {
		opts = append(opts, bus.WithAttachments(call.Attachments...))
	}

	c.injectTracing(ctx, &opts)

	conn, err := c.getConn(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Release()

	ctxlog.Debug(ctx, c.log.Logger(), "sending RPC request",
		log.String("proxy", call.SelectedProxy),
		log.String("request_id", call.CallID.String()),
	)

	start := time.Now()
	err = conn.Send(ctx, "ApiService", string(call.Method), call.Req, rsp, opts...)
	duration := time.Since(start)

	ctxlog.Debug(ctx, c.log.Logger(), "received RPC response",
		log.String("proxy", call.SelectedProxy),
		log.String("request_id", call.CallID.String()),
		log.Bool("ok", err == nil),
		log.Duration("duration", duration))

	if errors.Is(err, bus.ErrConnClosed) {
		conn.Discard()
	}

	return err
}

func (c *client) invokeInTx(
	ctx context.Context,
	call *Call,
	rsp proto.Message,
	opts ...bus.SendOption,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tx, err := c.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	commandCall := *call
	commandCall.DisableRetries = true
	if err := tx.(*TxInterceptor).Invoke(ctx, &commandCall, rsp, opts...); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (c *client) requestCredentials(ctx context.Context) (yt.Credentials, error) {
	if creds := yt.ContextCredentials(ctx); creds != nil {
		return creds, nil
	}

	if c.conf.TVMFn != nil {
		ticket, err := c.conf.TVMFn(ctx)
		if err != nil {
			return nil, err
		}

		credentials := &yt.ServiceTicketCredentials{Ticket: ticket}
		return credentials, nil
	}

	return c.credentials, nil
}

func (c *client) getConn(ctx context.Context, addr string) (*Conn, error) {
	dial, ok := GetDialer(ctx)
	if ok {
		conn := dial(ctx, addr)
		wrapped := NewConn(addr, conn, nil)
		return wrapped, nil
	}
	return c.connPool.Conn(ctx, addr)
}

func (c *client) Stop() {
	c.connPool.Stop()
	c.stop.Stop()
}

func (c *client) startCall() *Call {
	bf := backoff.NewExponentialBackOff()
	bf.MaxElapsedTime = c.conf.GetLightRequestTimeout()
	return &Call{
		Backoff: bf,
	}
}

func (c *client) injectTracing(ctx context.Context, opts *[]bus.SendOption) {
	if c.conf.TraceFn == nil {
		return
	}

	traceID, spanID, flags, ok := c.conf.TraceFn(ctx)
	if !ok {
		return
	}

	*opts = append(*opts, bus.WithTracing(traceID, spanID, flags))
}

// LockRows wraps encoder's implementation with transaction.
func (c *client) LockRows(
	ctx context.Context,
	path ypath.Path,
	locks []string,
	lockType yt.LockType,
	keys []any,
	opts *yt.LockRowsOptions,
) (err error) {
	if opts == nil {
		opts = &yt.LockRowsOptions{}
	}
	if opts.TransactionOptions == nil {
		opts.TransactionOptions = &yt.TransactionOptions{}
	}

	if len(keys) == 0 {
		return nil
	}

	var zero yt.TxID
	if opts.TransactionID != zero {
		return c.Encoder.LockRows(ctx, path, locks, lockType, keys, opts)
	}

	tx, err := c.BeginTabletTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Abort()

	opts.TransactionID = tx.ID()

	err = c.Encoder.LockRows(ctx, path, locks, lockType, keys, opts)
	if err != nil {
		return err
	}

	return tx.Commit()
}

type rowBatch struct {
	rows        []any
	rowCount    int
	attachments [][]byte
	descriptor  *rpc_proxy.TRowsetDescriptor
}

func (b *rowBatch) Write(row any) error {
	b.rows = append(b.rows, row)
	b.rowCount++
	return nil
}

func (b *rowBatch) Commit() error {
	if len(b.rows) == 0 {
		return nil
	}

	var err error
	b.attachments, b.descriptor, err = encodeToWire(b.rows)
	b.rows = nil
	return err
}

func (b *rowBatch) Rollback() error {
	return nil
}

func (b *rowBatch) Len() int {
	var l int
	for _, b := range b.attachments {
		l += len(b)
	}
	return l
}

func (b *rowBatch) Batch() yt.RowBatch {
	if b.rows != nil {
		panic("reading unfinished batch")
	}

	return b
}

func (c *client) NewRowBatchWriter() yt.RowBatchWriter {
	return &rowBatch{}
}

func buildBatch(rows []any) (yt.RowBatch, error) {
	var b rowBatch
	if len(rows) == 0 {
		return &b, nil
	}

	var err error
	b.attachments, b.descriptor, err = encodeToWire(rows)
	b.rowCount = len(rows)
	return &b, err
}

// PushQueueProducer wraps encoder's implementation with transaction.
func (c *client) PushQueueProducer(
	ctx context.Context,
	producerPath ypath.Path,
	queuePath ypath.Path,
	sessionID string,
	epoch int64,
	rows []any,
	opts *yt.PushQueueProducerOptions,
) (result *yt.PushQueueProducerResult, err error) {
	batch, err := buildBatch(rows)
	if err != nil {
		return nil, err
	}

	return c.PushQueueProducerBatch(ctx, producerPath, queuePath, sessionID, epoch, batch, opts)
}

func (c *client) PushQueueProducerBatch(
	ctx context.Context,
	producerPath ypath.Path,
	queuePath ypath.Path,
	sessionID string,
	epoch int64,
	rowBatch yt.RowBatch,
	opts *yt.PushQueueProducerOptions,
) (result *yt.PushQueueProducerResult, err error) {
	if opts == nil {
		opts = &yt.PushQueueProducerOptions{}
	}
	if opts.TransactionOptions == nil {
		opts.TransactionOptions = &yt.TransactionOptions{}
	}

	var zero yt.TxID
	if opts.TransactionID != zero {
		return c.Encoder.PushQueueProducerBatch(ctx, producerPath, queuePath, sessionID, epoch, rowBatch, opts)
	}

	tx, err := c.BeginTabletTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Abort()

	opts.TransactionID = tx.ID()

	result, err = tx.PushQueueProducerBatch(ctx, producerPath, queuePath, sessionID, epoch, rowBatch, opts)
	if err != nil {
		return result, err
	}

	return result, tx.Commit()
}

// InsertRows wraps encoder's implementation with transaction.
func (c *client) InsertRows(
	ctx context.Context,
	path ypath.Path,
	rows []any,
	opts *yt.InsertRowsOptions,
) (err error) {
	if len(rows) == 0 {
		return nil
	}

	batch, err := buildBatch(rows)
	if err != nil {
		return err
	}

	return c.InsertRowBatch(ctx, path, batch, opts)
}

func (c *client) InsertRowBatch(
	ctx context.Context,
	path ypath.Path,
	rowBatch yt.RowBatch,
	opts *yt.InsertRowsOptions,
) (err error) {
	if opts == nil {
		opts = &yt.InsertRowsOptions{}
	}
	if opts.TransactionOptions == nil {
		opts.TransactionOptions = &yt.TransactionOptions{}
	}

	var zero yt.TxID
	if opts.TransactionID != zero {
		return c.Encoder.InsertRowBatch(ctx, path, rowBatch, opts)
	}

	tx, err := c.BeginTabletTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Abort()

	opts.TransactionID = tx.ID()

	err = tx.InsertRowBatch(ctx, path, rowBatch, opts)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// DeleteRows wraps encoder's implementation with transaction.
func (c *client) DeleteRows(
	ctx context.Context,
	path ypath.Path,
	keys []any,
	opts *yt.DeleteRowsOptions,
) (err error) {
	if opts == nil {
		opts = &yt.DeleteRowsOptions{}
	}
	if opts.TransactionOptions == nil {
		opts.TransactionOptions = &yt.TransactionOptions{}
	}

	if len(keys) == 0 {
		return nil
	}

	var zero yt.TxID
	if opts.TransactionID != zero {
		return c.Encoder.DeleteRows(ctx, path, keys, opts)
	}

	tx, err := c.BeginTabletTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Abort()

	opts.TransactionID = tx.ID()

	err = c.Encoder.DeleteRows(ctx, path, keys, opts)
	if err != nil {
		return err
	}

	return tx.Commit()
}
