package bus

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/atomic"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/compression"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	"go.ytsaurus.tech/yt/go/proto/core/rpc"
	"go.ytsaurus.tech/yt/go/yterrors"
)

const (
	// defaultAcknowledgementTimeout is a default timeout used to cancel inflight unacked requests.
	defaultAcknowledgementTimeout = time.Second * 5

	// effectiveTimeout is a request timeout used when user has
	// neither set context deadline nor specified request timeout.
	effectiveTimeout = time.Hour * 24
)

// featureIDFormatter is a function that converts feature id to string.
type featureIDFormatter func(i int32) string

// defaultFeatureIDFormatter simply converts number to string.
func defaultFeatureIDFormatter(i int32) string {
	return fmt.Sprintf("%d", i)
}

type clientReq struct {
	id guid.GUID

	reqHeader *rpc.TRequestHeader
	rspHeader *rpc.TResponseHeader

	request, reply proto.Message

	reqAttachments [][]byte
	rspAttachments [][]byte

	acknowledgementTimeout *time.Duration
	acked                  atomic.Bool

	done chan error
}

type msgType uint32

const (
	maxInFlightRequests = 1000

	msgRequest        msgType = 0x69637072
	msgCancel         msgType = 0x63637072
	msgResponse       msgType = 0x6f637072
	msgStreamPayload  msgType = 0x70637072
	msgStreamFeedback msgType = 0x66637072
)

var (
	ytGoClient       = "YT go client"
	codecNone  int32 = 0
)

type ClientOption func(conn *ClientConn)

func WithLogger(l log.Logger) ClientOption {
	return func(conn *ClientConn) {
		conn.log = l
	}
}

func WithDefaultProtocolVersionMajor(v int32) ClientOption {
	return func(conn *ClientConn) {
		conn.defaultProtocolVersionMajor = v
	}
}

func WithFeatureIDFormatter(f featureIDFormatter) ClientOption {
	return func(conn *ClientConn) {
		conn.featureIDFormatter = f
	}
}

func WithEncryptionMode(em EncryptionMode) ClientOption {
	return func(conn *ClientConn) {
		conn.encryptionMode = em
	}
}

func WithTLSConfig(c *tls.Config) ClientOption {
	return func(conn *ClientConn) {
		conn.tlsConfig = c
	}
}

var ErrConnClosed = xerrors.NewSentinel("connection closed")

type ClientConn struct {
	l    sync.Mutex
	err  error
	stop chan struct{}
	reqs map[guid.GUID]*clientReq

	sendQueue, cancelQueue chan *clientReq

	// mu guards the following maps.
	mu sync.Mutex
	// unackedReqs stores unacked request id to packet id mapping.
	unackedReqs map[guid.GUID]guid.GUID
	// unackedPackets stores unacked packet id to request id mapping.
	unackedPackets map[guid.GUID]guid.GUID

	defaultProtocolVersionMajor int32
	featureIDFormatter          featureIDFormatter

	address string

	dialed chan struct{}
	bus    *Bus

	log log.Logger

	encryptionMode EncryptionMode
	tlsConfig      *tls.Config
}

func NewClient(ctx context.Context, address string, opts ...ClientOption) *ClientConn {
	l := &nop.Logger{}

	client := &ClientConn{
		reqs:        make(map[guid.GUID]*clientReq, maxInFlightRequests),
		sendQueue:   make(chan *clientReq, maxInFlightRequests),
		cancelQueue: make(chan *clientReq, maxInFlightRequests),
		stop:        make(chan struct{}),

		featureIDFormatter: defaultFeatureIDFormatter,

		unackedReqs:    make(map[guid.GUID]guid.GUID),
		unackedPackets: make(map[guid.GUID]guid.GUID),

		address: address,
		dialed:  make(chan struct{}),
		log:     l,
	}

	for _, opt := range opts {
		opt(client)
	}

	go client.dial(ctx)
	go client.runSender()
	go client.runReceiver()

	return client
}

func (c *ClientConn) Close() {
	c.l.Lock()
	defer c.l.Unlock()

	if c.bus != nil {
		c.bus.Close()
	}

	if c.err == nil {
		c.err = ErrConnClosed
		close(c.stop)
	}
}

func (c *ClientConn) Done() <-chan struct{} {
	return c.stop
}

func (c *ClientConn) Err() (err error) {
	c.l.Lock()
	err = c.err
	c.l.Unlock()
	return
}

func (c *ClientConn) fail(err error) {
	c.l.Lock()
	if c.err == nil {
		c.err = ErrConnClosed.Wrap(err)
		close(c.stop)
	}
	c.l.Unlock()
}

func (c *ClientConn) runSender() {
	select {
	case <-c.dialed:
	case <-c.stop:
		return
	}

	for {
		select {
		case req := <-c.cancelQueue:
			c.l.Lock()
			delete(c.reqs, req.id)
			c.l.Unlock()

			c.deleteUnackedReq(req.id)

			cancelReq := &clientReq{
				id: guid.New(),
				reqHeader: &rpc.TRequestHeader{
					RequestId: req.reqHeader.RequestId,
					Service:   req.reqHeader.Service,
					Method:    req.reqHeader.Method,
					RealmId:   req.reqHeader.RealmId,
					UserAgent: &ytGoClient,

					RequestCodec:  &codecNone,
					ResponseCodec: &codecNone,
				},
				request: &rpc.TRequestCancelationHeader{
					RequestId: req.reqHeader.RequestId,
					Service:   req.reqHeader.Service,
					Method:    req.reqHeader.Method,
					RealmId:   req.reqHeader.RealmId,
				},
			}

			msg, err := c.buildMsg(cancelReq, msgCancel)
			if err != nil {
				c.fail(err)
				return
			}

			packetID := guid.New()
			c.log.Debug("Sending cancel packet",
				log.String("id", packetID.String()),
				log.String("request_id", misc.NewGUIDFromProto(req.reqHeader.RequestId).String()))

			if err := c.bus.Send(packetID, msg, &busSendOptions{}); err != nil {
				c.fail(err)
				return
			}

		case req := <-c.sendQueue:
			msg, err := c.buildMsg(req, msgRequest)
			if err != nil {
				req.done <- err
				continue
			}

			c.l.Lock()
			c.reqs[req.id] = req
			c.l.Unlock()

			opts := &busSendOptions{}
			if req.acknowledgementTimeout != nil {
				opts.DeliveryTrackingLevel = DeliveryTrackingLevelFull
			}

			packetID := guid.New()
			if req.acknowledgementTimeout != nil {
				c.addUnackedReq(req.id, packetID)
			}

			c.log.Debug("Sending packet",
				log.String("id", packetID.String()),
				log.String("request_id", misc.NewGUIDFromProto(req.reqHeader.RequestId).String()))

			if err := c.bus.Send(packetID, msg, opts); err != nil {
				c.fail(err)
				return
			}

		case <-c.stop:
			return
		}
	}
}

func (c *ClientConn) dial(ctx context.Context) {
	opts := Options{
		Address:        c.address,
		Logger:         c.log,
		EncryptionMode: c.encryptionMode,
		TLSConfig:      c.tlsConfig,
	}
	bus, err := Dial(ctx, opts)
	if err != nil {
		c.fail(err)
		return
	}

	c.l.Lock()
	if c.err != nil {
		bus.Close()
	} else {
		c.bus = bus
	}
	c.l.Unlock()

	close(c.dialed)
}

func (c *ClientConn) buildMsg(req *clientReq, msgType msgType) ([][]byte, error) {
	msg := make([][]byte, 2+len(req.reqAttachments))

	var err error
	var protoHeader []byte
	if protoHeader, err = proto.Marshal(req.reqHeader); err != nil {
		return nil, err
	}

	msg[0] = make([]byte, 4, 4+len(protoHeader))
	binary.LittleEndian.PutUint32(msg[0], uint32(msgType))
	msg[0] = append(msg[0], protoHeader...)

	data, err := proto.Marshal(req.request)
	if err != nil {
		return nil, err
	}

	codecID := compression.CodecIDNone
	if req.reqHeader.RequestCodec != nil {
		codecID = compression.CodecID(*req.reqHeader.RequestCodec)
	}

	codec := compression.NewCodec(codecID)
	compressed, err := codec.Compress(data)
	if err != nil {
		return nil, err
	}
	msg[1] = compressed

	for i, a := range req.reqAttachments {
		compressed, err := codec.Compress(a)
		if err != nil {
			return nil, err
		}
		msg[2+i] = compressed
	}

	return msg, nil
}

func (c *ClientConn) handleAck(packetID guid.GUID) {
	reqID, ok := c.deleteUnackedPacket(packetID)
	if !ok {
		return
	}

	c.l.Lock()
	req, ok := c.reqs[reqID]
	c.l.Unlock()

	if !ok {
		return
	}

	req.acked.Store(true)
}

func (c *ClientConn) handleMsg(msg [][]byte) error {
	if len(msg) == 0 {
		return fmt.Errorf("bus: received empty message")
	}

	if len(msg[0]) < 4 {
		return fmt.Errorf("bus: message type is missing")
	}

	if typ := msgType(binary.LittleEndian.Uint32(msg[0][:4])); typ != msgResponse {
		c.log.Warnf("ignoring message of unexpected type: got %x, want %x", typ, msgResponse)
		return nil
	}

	var rsp rpc.TResponseHeader
	if err := proto.Unmarshal(msg[0][4:], &rsp); err != nil {
		return err
	}

	if rsp.RequestId == nil {
		return fmt.Errorf("bus: response is missing request_id")
	}

	reqID := misc.NewGUIDFromProto(rsp.RequestId)

	c.l.Lock()
	req, ok := c.reqs[reqID]
	delete(c.reqs, reqID)
	c.l.Unlock()

	if !ok {
		c.log.Warn("request not found; ignoring", log.String("request_id", reqID.String()))
		return nil
	}

	req.rspHeader = &rsp
	if replyErr := misc.NewErrorFromProto(req.rspHeader.Error); replyErr != nil {
		req.done <- replyErr
		return nil
	}

	if len(msg) < 2 {
		req.done <- fmt.Errorf("bus: reply body is missing")
		return nil
	}

	codecID := compression.CodecIDNone
	if req.reqHeader.ResponseCodec != nil {
		codecID = compression.CodecID(*req.reqHeader.ResponseCodec)
	}

	codec := compression.NewCodec(codecID)
	reply, err := codec.Decompress(msg[1])
	if err != nil {
		req.done <- err
		return nil
	}

	if err := proto.Unmarshal(reply, req.reply); err != nil {
		req.done <- err
		return nil
	}

	for _, a := range msg[2:] {
		decompressed, err := codec.Decompress(a)
		if err != nil {
			req.done <- err
			return nil
		}
		req.rspAttachments = append(req.rspAttachments, decompressed)
	}

	req.done <- nil
	return nil
}

func (c *ClientConn) runReceiver() {
	select {
	case <-c.dialed:
	case <-c.stop:
		return
	}

	for {
		msg, err := c.bus.Receive()
		if err != nil {
			c.fail(err)
			return
		}

		switch msg.fixHeader.typ {
		case packetAck:
			c.handleAck(msg.fixHeader.packetID)

		default:
			if err := c.handleMsg(msg.parts); err != nil {
				c.fail(err)
				return
			}
		}
	}
}

func (c *ClientConn) Send(
	ctx context.Context,
	service, method string,
	request, reply proto.Message,
	opts ...SendOption,
) error {
	reqID := guid.New()
	req := &clientReq{
		id:      reqID,
		request: request,
		reply:   reply,

		reqHeader: &rpc.TRequestHeader{
			RequestId:            misc.NewProtoFromGUID(reqID),
			Service:              &service,
			Method:               &method,
			ProtocolVersionMajor: &c.defaultProtocolVersionMajor,
			UserAgent:            &ytGoClient,

			RequestCodec:  &codecNone,
			ResponseCodec: &codecNone,
		},

		acknowledgementTimeout: ptr.Duration(defaultAcknowledgementTimeout),

		done: make(chan error, 1),
	}

	for _, opt := range opts {
		opt.before(req)
	}

	userDeadline, hasUserDeadline := ctx.Deadline()
	if hasUserDeadline && req.reqHeader.Timeout == nil {
		if timeout := time.Until(userDeadline); timeout > 0 {
			req.reqHeader.Timeout = ptr.Int64(durationToMicroseconds(timeout))
		}
	}

	select {
	case c.sendQueue <- req:

	case <-c.stop:
		return c.err

	case <-ctx.Done():
		return ctx.Err()
	}

	reqCtx := ctx
	if !hasUserDeadline {
		effectiveTimeout := effectiveTimeout
		if req.reqHeader.Timeout != nil {
			effectiveTimeout = microsecondsToDuration(*req.reqHeader.Timeout)
		}
		var cancel func()
		reqCtx, cancel = context.WithTimeout(ctx, effectiveTimeout)
		defer cancel()
	}

	var ackTimeout <-chan time.Time
	if req.acknowledgementTimeout != nil {
		t := time.NewTimer(*req.acknowledgementTimeout)
		defer func() { _ = t.Stop() }()

		ackTimeout = t.C
	}

loop:
	select {
	case <-c.stop:
		return c.err

	case err := <-req.done:
		if err != nil {
			c.enrichClientRequestErrorWithFeatureName(err)
			return err
		}

	case <-ackTimeout:
		if req.acked.Load() {
			goto loop
		}

		c.finishReq(req)

		err := yterrors.Err(yterrors.CodeTimeout, "Request acknowledgement timed out")
		c.log.Error("Request acknowledgement timeout", log.Error(err))
		return err

	case <-reqCtx.Done():
		c.finishReq(req)

		if errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
			msg := "Request timed out or timer was aborted"
			if ctx.Err() == nil {
				msg = "Request timed out"
			}
			err := yterrors.Err(yterrors.CodeTimeout, msg)
			c.log.Error("Request timeout", log.Error(err))
			return err
		} else if errors.Is(reqCtx.Err(), context.Canceled) {
			err := yterrors.Err(yterrors.CodeCanceled, "Request canceled")
			c.log.Error("Context canceled", log.Error(err))
			return err
		}

		return ctx.Err()
	}

	for _, opt := range opts {
		opt.after(req)
	}

	return nil
}

func (c *ClientConn) finishReq(req *clientReq) {
	select {
	case c.cancelQueue <- req:
	default:
		c.l.Lock()
		delete(c.reqs, req.id)
		c.l.Unlock()

		c.deleteUnackedReq(req.id)
	}
}

func (c *ClientConn) enrichClientRequestErrorWithFeatureName(err error) {
	yterr, ok := err.(*yterrors.Error)
	if !ok {
		return
	}

	if yterr.Code != yterrors.CodeUnsupportedServerFeature ||
		!yterr.HasAttr(string(AttributeKeyFeatureID)) ||
		yterr.HasAttr(string(AttributeKeyFeatureName)) {
		return
	}

	featureID := int32(yterr.Attributes[string(AttributeKeyFeatureID)].(int64))
	yterr.AddAttr(string(AttributeKeyFeatureName), c.featureIDFormatter(featureID))
}

func (c *ClientConn) addUnackedReq(reqID, packetID guid.GUID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.unackedReqs[reqID] = packetID
	c.unackedPackets[packetID] = reqID
}

func (c *ClientConn) deleteUnackedReq(reqID guid.GUID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	packetID, ok := c.unackedReqs[reqID]
	if ok {
		delete(c.unackedPackets, packetID)
	}
	delete(c.unackedReqs, reqID)
}

func (c *ClientConn) deleteUnackedPacket(packetID guid.GUID) (guid.GUID, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	reqID, ok := c.unackedPackets[packetID]
	if !ok {
		return guid.GUID{}, false
	}

	delete(c.unackedPackets, packetID)
	delete(c.unackedReqs, reqID)

	return reqID, true
}

func durationToMicroseconds(d time.Duration) int64 {
	return int64(d / time.Microsecond)
}

func microsecondsToDuration(us int64) time.Duration {
	return time.Microsecond * time.Duration(us)
}
