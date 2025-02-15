package httpclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/library/go/blockcodecs"
	_ "go.ytsaurus.tech/library/go/blockcodecs/all"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func decodeYTErrorFromHeaders(h http.Header) (ytErr *yterrors.Error, err error) {
	header := h.Get("X-YT-Error")
	if header == "" {
		return nil, nil
	}

	ytErr = &yterrors.Error{}
	if decodeErr := json.Unmarshal([]byte(header), ytErr); decodeErr != nil {
		err = xerrors.Errorf("yt: malformed 'X-YT-Error' header: %w", decodeErr)
	}

	return
}

type httpClient struct {
	internal.Encoder

	requestLogger   *internal.LoggingInterceptor
	requestTracer   *internal.TracingInterceptor
	mutationRetrier *internal.MutationRetrier
	readRetrier     *internal.Retrier

	clusterURL yt.ClusterURL
	netDialer  *net.Dialer
	httpClient *http.Client
	log        log.Structured
	tracer     opentracing.Tracer
	config     *yt.Config
	stop       *internal.StopGroup

	credentials yt.Credentials

	proxySet *internal.ProxySet
}

func getTVMOnlyPort(config *yt.Config) int {
	if config.UseTLS {
		return yt.TVMOnlyHTTPSProxyPort
	}
	return yt.TVMOnlyHTTPProxyPort
}

func (c *httpClient) listHeavyProxies() ([]string, error) {
	if !c.stop.TryAdd() {
		return nil, fmt.Errorf("client is stopped")
	}
	defer c.stop.Done()

	v := url.Values{}
	if c.config.ProxyRole != "" {
		v.Add("role", c.config.ProxyRole)
	}

	var resolveURL url.URL
	resolveURL.Scheme = c.clusterURL.Scheme
	resolveURL.Host = c.clusterURL.Address
	resolveURL.Path = c.config.HostsPath
	if resolveURL.Path == "" {
		resolveURL.Path = "hosts"
	}
	resolveURL.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", resolveURL.String(), nil)
	if err != nil {
		return nil, err
	}

	var rsp *http.Response
	rsp, err = c.httpClient.Do(req.WithContext(c.stop.Context()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rsp.Body.Close() }()

	if rsp.StatusCode != 200 {
		return nil, unexpectedStatusCode(rsp)
	}

	var proxies []string
	if err = json.NewDecoder(rsp.Body).Decode(&proxies); err != nil {
		return nil, err
	}

	if len(proxies) == 0 {
		return nil, xerrors.New("proxy list is empty")
	}

	if c.config.UseTVMOnlyEndpoint {
		port := getTVMOnlyPort(c.config)
		for i, proxy := range proxies {
			proxies[i] = net.JoinHostPort(proxy, fmt.Sprint(port))
		}
	}

	return proxies, nil
}

func (c *httpClient) pickHeavyProxy(ctx context.Context) (string, error) {
	if proxy, ok := GetHeavyProxyOverride(ctx); ok {
		return proxy, nil
	}

	if c.clusterURL.DisableDiscovery {
		return c.clusterURL.Address, nil
	}

	proxy, err := c.proxySet.PickRandom(ctx)
	if err != nil {
		return "", err
	}

	return proxy, nil
}

func (c *httpClient) writeParams(req *http.Request, call *internal.Call) error {
	var params bytes.Buffer

	w := yson.NewWriterFormat(&params, yson.FormatText)
	w.BeginMap()
	call.Params.MarshalHTTP(w)
	w.EndMap()
	if err := w.Finish(); err != nil {
		return err
	}

	h := req.Header
	h.Add("X-YT-Header-Format", "yson")
	if req.Method == http.MethodPost && req.Body == http.NoBody {
		req.Body = io.NopCloser(&params)
		req.ContentLength = int64(params.Len())
		req.GetBody = func() (body io.ReadCloser, e error) {
			return io.NopCloser(&params), nil
		}
	} else {
		h.Add("X-YT-Parameters", params.String())
	}
	h.Add("X-YT-Correlation-ID", call.CallID.String())
	h.Set("User-Agent", "go-yt-client")

	return nil
}

func (c *httpClient) injectTracing(ctx context.Context, req *http.Request) {
	if c.config.TraceFn == nil {
		return
	}

	traceID, spanID, flags, ok := c.config.TraceFn(ctx)
	if !ok {
		return
	}

	req.Header.Add("traceparent", fmt.Sprintf("%s-%016x-%02x", traceID.HexString(), spanID, flags))
}

func (c *httpClient) newHTTPRequest(ctx context.Context, call *internal.Call, body io.Reader) (req *http.Request, err error) {
	var address string
	if call.RequestedProxy != "" {
		address = call.RequestedProxy
		call.SelectedProxy = address
	} else if call.Params.HTTPVerb().IsHeavy() {
		address, err = c.pickHeavyProxy(ctx)
		if err != nil {
			return nil, err
		}
		call.SelectedProxy = address
	} else {
		address = c.clusterURL.Address
	}

	if body == nil {
		body = bytes.NewBuffer(call.YSONValue)
	}

	verb := call.Params.HTTPVerb()
	req, err = http.NewRequest(verb.HTTPMethod(), c.clusterURL.Scheme+"://"+address+call.APIPath+verb.String(), body)
	if err != nil {
		return
	}

	if err = c.writeParams(req, call); err != nil {
		return
	}

	c.injectTracing(ctx, req)

	if body != nil {
		req.Header.Add("X-YT-Input-Format", "yson")
	}
	req.Header.Add("X-YT-Header-Format", "<format=text>yson")
	req.Header.Add("X-YT-Output-Format", "yson")

	credentials, err := c.requestCredentials(ctx)
	if err != nil {
		return nil, err
	}

	if credentials != nil {
		credentials.Set(req)
	}

	if c.config.ImpersonationUser != "" {
		req.Header.Add("X-YT-User-Name", c.config.ImpersonationUser)
	}

	c.logRequest(ctx, req)
	return
}

func (c *httpClient) requestCredentials(ctx context.Context) (yt.Credentials, error) {
	if creds := yt.ContextCredentials(ctx); creds != nil {
		return creds, nil
	}

	if c.config.TVMFn != nil {
		ticket, err := c.config.TVMFn(ctx)
		if err != nil {
			return nil, err
		}
		credentials := &yt.ServiceTicketCredentials{Ticket: ticket}
		return credentials, nil
	}

	return c.credentials, nil
}

func (c *httpClient) logRequest(ctx context.Context, req *http.Request) {
	ctxlog.Debug(ctx, c.log.Logger(), "sending HTTP request",
		log.String("proxy", req.URL.Host))
}

func (c *httpClient) logResponse(ctx context.Context, rsp *http.Response) {
	ctxlog.Debug(ctx, c.log.Logger(), "received HTTP response",
		log.String("yt_proxy", rsp.Header.Get("X-YT-Proxy")),
		log.String("yt_request_id", rsp.Header.Get("X-YT-Request-Id")))
}

// unexpectedStatusCode is last effort attempt to get useful error message from a failed request.
func unexpectedStatusCode(rsp *http.Response) error {
	d := json.NewDecoder(rsp.Body)
	d.UseNumber()

	var ytErr yterrors.Error
	if err := d.Decode(&ytErr); err == nil {
		return &ytErr
	}

	return xerrors.Errorf("unexpected status code %d", rsp.StatusCode)
}

func (c *httpClient) readResult(rsp *http.Response) (res *internal.CallResult, err error) {
	defer func() { _ = rsp.Body.Close() }()

	res = &internal.CallResult{}

	var ytErr *yterrors.Error
	ytErr, err = decodeYTErrorFromHeaders(rsp.Header)
	if err != nil {
		return
	}
	if ytErr != nil {
		return nil, ytErr
	}

	if rsp.StatusCode/100 != 2 {
		return nil, unexpectedStatusCode(rsp)
	}

	res.YSONValue, err = io.ReadAll(rsp.Body)
	return
}

func (c *httpClient) startCall() *internal.Call {
	bf := backoff.NewExponentialBackOff()
	bf.MaxElapsedTime = c.config.GetLightRequestTimeout()
	return &internal.Call{
		Backoff: bf,
	}
}

func (c *httpClient) roundTrip(req *http.Request) (*http.Response, error) {
	rt, ok := GetRoundTripper(req.Context())
	if ok {
		return rt.RoundTrip(req)
	}

	return c.httpClient.Do(req)
}

func (c *httpClient) do(ctx context.Context, call *internal.Call) (res *internal.CallResult, err error) {
	var req *http.Request
	req, err = c.newHTTPRequest(ctx, call, nil)
	if err != nil {
		return nil, err
	}

	var rsp *http.Response
	rsp, err = c.roundTrip(req.WithContext(ctx))
	if err != nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
	}

	if err == nil {
		c.logResponse(ctx, rsp)
		res, err = c.readResult(rsp)
	}

	return
}

func (c *httpClient) doInTx(ctx context.Context, call *internal.Call) (res *internal.CallResult, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tx, err := c.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	commandCall := *call
	commandCall.DisableRetries = true
	res, err = tx.(*internal.TxInterceptor).Invoke(ctx, &commandCall)
	if err != nil {
		return
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return
}

type pipeWrapper struct {
	*io.PipeReader
	readStarted chan struct{}
}

func (w *pipeWrapper) Read(buf []byte) (int, error) {
	if w.readStarted != nil {
		close(w.readStarted)
		w.readStarted = nil
	}

	return w.PipeReader.Read(buf)
}

func (w *pipeWrapper) Close() error {
	return nil
}

func (c *httpClient) doWrite(ctx context.Context, call *internal.Call) (w io.WriteCloser, err error) {
	errChan := make(chan error, 1)
	readStarted := make(chan struct{})

	var (
		req *http.Request
		pr  *io.PipeReader
		pw  *io.PipeWriter
	)

	if call.RowBatch == nil {
		pr, pw = io.Pipe()
		req, err = c.newHTTPRequest(ctx, call, &pipeWrapper{pr, readStarted})
		if err != nil {
			return nil, err
		}
	} else {
		req, err = c.newHTTPRequest(ctx, call, bytes.NewBuffer(call.RowBatch.(*rowBatch).buf.Bytes()))
		if err != nil {
			return nil, err
		}
		close(readStarted)
	}

	switch c.config.GetClientCompressionCodec() {
	case yt.ClientCodecGZIP, yt.ClientCodecNone:
		// do nothing
	default:
		encoding, err := toHTTPEncoding(c.config.GetClientCompressionCodec())
		if err != nil {
			return nil, err
		}

		req.Header.Add("Content-Encoding", encoding)
	}

	go func() {
		defer close(errChan)
		closeErr := func(err error) {
			errChan <- err
			if pr != nil {
				_ = pr.CloseWithError(err)
			}
		}

		if call.WriteRspChan != nil {
			defer close(call.WriteRspChan)
		}

		rsp, err := c.roundTrip(req.WithContext(ctx))
		if err != nil {
			closeErr(err)
			return
		}

		c.logResponse(ctx, rsp)
		res, err := c.readResult(rsp)
		if err != nil {
			closeErr(err)
			return
		}
		if call.WriteRspChan != nil {
			call.WriteRspChan <- res
		}
	}()

	select {
	case err = <-errChan:
		return
	case <-readStarted:
	}

	if call.RowBatch == nil {
		hw := &httpWriter{w: pw, c: pw, errChan: errChan}
		w = hw

		switch c.config.GetClientCompressionCodec() {
		case yt.ClientCodecGZIP, yt.ClientCodecNone:
			// do nothing
		default:
			block, ok := c.config.GetClientCompressionCodec().BlockCodec()
			if !ok {
				err = fmt.Errorf("unsupported compression codec %d", c.config.GetClientCompressionCodec())
				return
			}

			codec := blockcodecs.FindCodecByName(block)
			if codec == nil {
				err = fmt.Errorf("unsupported compression codec %q", block)
				return
			}

			encoder := blockcodecs.NewEncoder(hw.w, codec)
			hw.w = encoder
			hw.c = &compressorCloser{enc: encoder, pw: pw}
		}
	} else {
		hw := &httpWriter{errChan: errChan}
		w = hw
	}

	return
}

type compressorCloser struct {
	enc io.WriteCloser
	pw  io.Closer
}

func (c *compressorCloser) Write(p []byte) (int, error) {
	return c.enc.Write(p)
}

func (c *compressorCloser) Close() error {
	if err := c.enc.Close(); err != nil {
		return err
	}

	if c.pw != nil {
		return c.pw.Close()
	}

	return nil
}

func (c *httpClient) doWriteRow(ctx context.Context, call *internal.Call) (w yt.TableWriter, err error) {
	var ww io.WriteCloser

	ctx, cancelFunc := context.WithCancel(ctx)
	ww, err = c.InvokeWrite(ctx, call)
	if err != nil {
		cancelFunc()
		return
	}

	w = newTableWriter(ww, cancelFunc)
	return
}

func (c *httpClient) doReadRow(ctx context.Context, call *internal.Call) (r yt.TableReader, err error) {
	if call.OnRspParams != nil {
		panic("call.OnRspParams is already set")
	}

	var rspParams *tableReaderRspParams
	call.OnRspParams = func(ys []byte) (err error) {
		rspParams, err = decodeRspParams(ys)
		return
	}

	var rr io.ReadCloser
	rr, err = c.InvokeRead(ctx, call)
	if err != nil {
		return
	}

	tr := newTableReader(rr)

	if rspParams != nil {
		if err := tr.setRspParams(rspParams); err != nil {
			return nil, xerrors.Errorf("invalid rsp params: %w", err)
		}
	}

	r = tr
	return
}

func toHTTPEncoding(c yt.ClientCompressionCodec) (string, error) {
	if block, ok := c.BlockCodec(); ok {
		return "z-" + block, nil
	}

	return "", fmt.Errorf("codec %d is not supported", c)
}

func (c *httpClient) doRead(ctx context.Context, call *internal.Call) (r io.ReadCloser, err error) {
	var req *http.Request
	req, err = c.newHTTPRequest(ctx, call, nil)
	if err != nil {
		return nil, err
	}

	switch c.config.GetClientCompressionCodec() {
	case yt.ClientCodecGZIP, yt.ClientCodecNone:
		// do nothing
	default:
		encoding, err := toHTTPEncoding(c.config.GetClientCompressionCodec())
		if err != nil {
			return nil, err
		}

		req.Header.Add("Accept-Encoding", encoding)
	}

	var rsp *http.Response
	rsp, err = c.roundTrip(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	c.logResponse(ctx, rsp)
	if rsp.StatusCode != 200 {
		_ = rsp.Body.Close()

		var callErr *yterrors.Error
		callErr, err = decodeYTErrorFromHeaders(rsp.Header)
		if err == nil && callErr != nil {
			err = callErr
		} else {
			err = unexpectedStatusCode(rsp)
		}
	}

	if err == nil && call.OnRspParams != nil {
		if p := rsp.Header.Get("X-YT-Response-Parameters"); p != "" {
			err = call.OnRspParams([]byte(p))
			if err != nil {
				_ = rsp.Body.Close()
			}
		}
	}

	if err != nil {
		return
	}

	br := &httpReader{
		body:       rsp.Body,
		bodyCloser: rsp.Body,
		rsp:        rsp,
	}
	r = br

	switch c.config.GetClientCompressionCodec() {
	case yt.ClientCodecGZIP, yt.ClientCodecNone:
		// do nothing
	default:
		if rspEncoding := rsp.Header.Get("Content-Encoding"); rspEncoding != req.Header.Get("Accept-Encoding") {
			_ = rsp.Body.Close()
			err = fmt.Errorf("unexpected response encoding %q != %q", rspEncoding, req.Header.Get("Accept-Encoding"))
			return
		}

		d := blockcodecs.NewDecoder(br.body)
		d.SetCheckUnderlyingEOF(true)
		br.body = d
	}

	return
}

func (c *httpClient) BeginTx(ctx context.Context, options *yt.StartTxOptions) (yt.Tx, error) {
	return internal.BeginTx(ctx, c.Encoder, c.log, c.stop, c.config, options)
}

func (c *httpClient) AttachTx(ctx context.Context, txID yt.TxID, options *yt.AttachTxOptions) (yt.Tx, error) {
	return internal.AttachTx(ctx, c.Encoder, c.log, c.stop, c.config, txID, options)
}

func (c *httpClient) Stop() {
	c.stop.Stop()
}

func (c *httpClient) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	for i := 0; ; i++ {
		conn, err := c.netDialer.DialContext(ctx, network, addr)
		if err != nil {
			var temporary interface{ Temporary() bool }
			if errors.As(err, &temporary) && temporary.Temporary() && i < 3 {
				continue
			}

			var timeout interface{ Timeout() bool }
			if errors.As(err, &timeout) && timeout.Timeout() && i < 3 {
				continue
			}

			return nil, err
		}

		return conn, nil
	}
}

func NewHTTPClient(c *yt.Config) (yt.Client, error) {
	var client httpClient

	clusterURL, err := c.GetCusterURL()
	if err != nil {
		return nil, err
	}

	certPool, err := internal.NewCertPool()
	if err != nil {
		return nil, err
	}

	if len(c.CertificateAuthorityData) > 0 {
		if ok := certPool.AppendCertsFromPEM(c.CertificateAuthorityData); !ok {
			return nil, errors.New("invalid PEM encoded certificate")
		}
	}

	client.log = c.GetLogger()
	client.tracer = c.GetTracer()

	client.config = c
	client.clusterURL = clusterURL

	client.netDialer = &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	client.httpClient = &http.Client{
		Transport: &http.Transport{
			DialContext: client.dialContext,

			MaxIdleConns:        0,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     30 * time.Second,

			TLSHandshakeTimeout: 10 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs: certPool,
			},

			DisableCompression: c.GetClientCompressionCodec() != yt.ClientCodecGZIP,
		},
	}
	client.stop = internal.NewStopGroup()
	client.proxySet = &internal.ProxySet{UpdateFn: client.listHeavyProxies}

	client.Encoder.StartCall = client.startCall
	client.Encoder.Invoke = client.do
	client.Encoder.InvokeInTx = client.doInTx
	client.Encoder.InvokeRead = client.doRead
	client.Encoder.InvokeReadRow = client.doReadRow
	client.Encoder.InvokeWrite = client.doWrite
	client.Encoder.InvokeWriteRow = client.doWriteRow

	client.mutationRetrier = &internal.MutationRetrier{Log: client.log}
	client.readRetrier = &internal.Retrier{Config: client.config, Log: client.log}
	client.requestLogger = &internal.LoggingInterceptor{Structured: client.log}
	client.requestTracer = &internal.TracingInterceptor{Tracer: client.tracer}
	proxyBouncer := &internal.ProxyBouncer{Log: client.log, ProxySet: client.proxySet}
	errorWrapper := &internal.ErrorWrapper{}

	client.Encoder.Invoke = client.Encoder.Invoke.
		Wrap(proxyBouncer.Intercept).
		Wrap(client.requestLogger.Intercept).
		Wrap(client.requestTracer.Intercept).
		Wrap(client.mutationRetrier.Intercept).
		Wrap(client.readRetrier.Intercept).
		Wrap(errorWrapper.Intercept)

	client.Encoder.InvokeInTx = client.Encoder.InvokeInTx.
		Wrap(client.readRetrier.InterceptInTx)

	client.Encoder.InvokeRead = client.Encoder.InvokeRead.
		Wrap(proxyBouncer.Read).
		Wrap(client.requestLogger.Read).
		Wrap(client.requestTracer.Read).
		Wrap(client.readRetrier.Read).
		Wrap(errorWrapper.Read)

	client.Encoder.InvokeWrite = client.Encoder.InvokeWrite.
		Wrap(proxyBouncer.Write).
		Wrap(client.requestLogger.Write).
		Wrap(client.requestTracer.Write).
		Wrap(client.readRetrier.Write).
		Wrap(errorWrapper.Write)

	if c.TVMFn == nil {
		if c.Credentials != nil {
			client.credentials = c.Credentials
		} else if token := c.GetToken(); token != "" {
			client.credentials = &yt.TokenCredentials{Token: token}
		}
	}

	return &client, nil
}
