package bus

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/crc64"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/core/bus"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
)

type AttributeKey string

const (
	AttributeKeyFeatureID   AttributeKey = "feature_id"
	AttributeKeyFeatureName AttributeKey = "feature_name"
)

type EncryptionMode int

const (
	// TODO(khlebnikov) Add enum into proto file.
	EncryptionModeDisabled EncryptionMode = 0
	EncryptionModeOptional EncryptionMode = 1
	EncryptionModeRequired EncryptionMode = 2
)

type Options struct {
	Address string
	Logger  log.Logger

	EncryptionMode EncryptionMode
	TLSConfig      *tls.Config
}

// NOTE: See yt/yt/core/bus/tcp/packet.cpp

type packetType uint16
type packetFlags uint16

const (
	packetMessage = packetType(0)
	packetAck     = packetType(1)
	packetSSLAck  = packetType(2)

	packetFlagsNone                   = packetFlags(0x0000)
	packetFlagsRequestAcknowledgement = packetFlags(0x0001)

	packetSignature    = uint32(0x78616d4f)
	handshakeSignature = uint32(0x68737562)
	nullPartSize       = uint32(0xffffffff)
	maxPartSize        = 512 * 1024 * 1024
	maxPartCount       = 64
	fixHeaderSize      = 36
)

type fixedHeader struct {
	signature uint32
	typ       packetType
	flags     packetFlags
	packetID  guid.GUID
	partCount uint32
	checksum  uint64
}

func (p *fixedHeader) isVariablePacket() bool {
	// NOTE: Message packets always have variable header, other only when have payload.
	return p.typ == packetMessage || p.partCount > 0
}

func (p *fixedHeader) data(computeCRC bool) []byte {
	res := make([]byte, fixHeaderSize)
	binary.LittleEndian.PutUint32(res[0:4], p.signature)
	binary.LittleEndian.PutUint16(res[4:6], uint16(p.typ))
	binary.LittleEndian.PutUint16(res[6:8], uint16(p.flags))

	a, b, c, d := p.packetID.Parts()
	binary.LittleEndian.PutUint32(res[8:12], a)
	binary.LittleEndian.PutUint32(res[12:16], b)
	binary.LittleEndian.PutUint32(res[16:20], c)
	binary.LittleEndian.PutUint32(res[20:24], d)

	binary.LittleEndian.PutUint32(res[24:28], p.partCount)

	if computeCRC {
		binary.LittleEndian.PutUint64(res[28:36], crc64.Checksum(res[0:28]))
	}

	return res
}

type variableHeader struct {
	sizes          []uint32
	checksums      []uint64
	headerChecksum uint64
}

func (p *variableHeader) dataSize() int {
	return (8+4)*len(p.sizes) + 8
}

func (p *variableHeader) data(computeCRC bool) []byte {
	res := make([]byte, p.dataSize())
	c := 0
	for _, p := range p.sizes {
		binary.LittleEndian.PutUint32(res[c:c+4], p)
		c = c + 4
	}

	for _, p := range p.checksums {
		binary.LittleEndian.PutUint64(res[c:c+8], p)
		c = c + 8
	}

	if computeCRC {
		binary.LittleEndian.PutUint64(res[c:c+8], crc64.Checksum(res[:c]))
	}
	return res
}

type busMsg struct {
	fixHeader fixedHeader
	varHeader variableHeader
	parts     [][]byte
}

func newMessagePacket(id guid.GUID, data [][]byte, flags packetFlags, computeCRC bool) busMsg {
	hdr := fixedHeader{
		typ:       packetMessage,
		flags:     flags,
		partCount: uint32(len(data)),
		packetID:  id,
		checksum:  0,
		signature: packetSignature,
	}

	varHdr := variableHeader{
		checksums: make([]uint64, len(data)),
		sizes:     make([]uint32, len(data)),
	}

	for i, p := range data {
		if p == nil {
			varHdr.sizes[i] = nullPartSize
		} else {
			varHdr.sizes[i] = uint32(len(p))
		}

		if computeCRC {
			varHdr.checksums[i] = crc64.Checksum(p)
		}
	}

	return busMsg{
		fixHeader: hdr,
		varHeader: varHdr,
		parts:     data,
	}
}

func newSSLAckPacket(id guid.GUID) busMsg {
	hdr := fixedHeader{
		typ:       packetSSLAck,
		flags:     packetFlagsNone,
		partCount: 0,
		packetID:  id,
		checksum:  0,
		signature: packetSignature,
	}

	return busMsg{
		fixHeader: hdr,
		varHeader: variableHeader{},
		parts:     nil,
	}
}

func (p *busMsg) writeTo(w io.Writer) (int, error) {
	n := 0

	l, err := w.Write(p.fixHeader.data(true))
	if err != nil {
		return n + l, err
	}
	n += l

	if p.fixHeader.isVariablePacket() {
		l, err = w.Write(p.varHeader.data(true))
		if err != nil {
			return n + l, err
		}
		n += l

		for _, p := range p.parts {
			l, err := w.Write(p)
			if err != nil {
				return n + l, err
			}
			n += l
		}
	}

	return n, nil
}

type Bus struct {
	id      guid.GUID
	options Options
	conn    net.Conn
	logger  log.Logger
	once    sync.Once
	closed  atomic.Bool
}

func NewBus(conn net.Conn, options Options) *Bus {
	bus := &Bus{
		id:      guid.New(),
		options: options,
		conn:    conn,
	}

	logger := options.Logger
	if logger == nil {
		logger = &nop.Logger{}
	}
	bus.setLogger(logger)

	return bus
}

func (c *Bus) setLogger(l log.Logger) {
	c.logger = log.With(l, log.Any("busID", c.id))
}

func (c *Bus) Close() {
	c.once.Do(func() {
		c.closed.Store(true)
		if err := c.conn.Close(); err != nil {
			c.logger.Error("Connection close error", log.Error(err))
		}

		c.logger.Debug("Bus closed")
	})
}

type DeliveryTrackingLevel int

const (
	DeliveryTrackingLevelNone      DeliveryTrackingLevel = 0
	DeliveryTrackingLevelErrorOnly DeliveryTrackingLevel = 1
	DeliveryTrackingLevelFull      DeliveryTrackingLevel = 2
)

type busSendOptions struct {
	DeliveryTrackingLevel DeliveryTrackingLevel
}

func (c *Bus) Send(packetID guid.GUID, packetData [][]byte, opts *busSendOptions) error {
	flags := packetFlagsNone
	if opts.DeliveryTrackingLevel == DeliveryTrackingLevelFull {
		flags = packetFlagsRequestAcknowledgement
	}

	packet := newMessagePacket(packetID, packetData, flags, true)
	l, err := packet.writeTo(c.conn)
	if err != nil {
		c.logger.Error("Unable to send packet", log.Error(err))
		return err
	}

	c.logger.Debug("Packet sent",
		log.String("id", packet.fixHeader.packetID.String()),
		log.Any("bytes", l))

	return nil
}

func (c *Bus) Receive() (busMsg, error) {
	packet, err := c.receive()
	if err != nil {
		if !c.closed.Load() || !errors.Is(err, net.ErrClosed) {
			c.logger.Error("Receive error", log.Error(err))
		} else {
			c.logger.Debug("Unable to receive from closed connection", log.Error(err))
		}
		return busMsg{}, err
	}

	c.logger.Debug("Packet received",
		log.String("id", packet.fixHeader.packetID.String()),
		log.Any("type", packet.fixHeader.typ),
		log.Int16("flags", int16(packet.fixHeader.flags)))

	return packet, nil
}

func (c *Bus) receive() (busMsg, error) {
	rawFixHeader := make([]byte, fixHeaderSize)
	if _, err := io.ReadFull(c.conn, rawFixHeader); err != nil {
		return busMsg{}, fmt.Errorf("bus: error reading fix header: %w", err)
	}

	fixHeader := fixedHeader{}
	fixHeader.signature = binary.LittleEndian.Uint32(rawFixHeader[0:4])
	if fixHeader.signature != packetSignature {
		return busMsg{}, errors.New("bus: signature mismatch")
	}

	fixHeader.typ = packetType(binary.LittleEndian.Uint16(rawFixHeader[4:6]))
	fixHeader.flags = packetFlags(binary.LittleEndian.Uint16(rawFixHeader[6:8]))
	fixHeader.packetID = guid.FromParts(
		binary.LittleEndian.Uint32(rawFixHeader[8:12]),
		binary.LittleEndian.Uint32(rawFixHeader[12:16]),
		binary.LittleEndian.Uint32(rawFixHeader[16:20]),
		binary.LittleEndian.Uint32(rawFixHeader[20:24]),
	)
	fixHeader.partCount = binary.LittleEndian.Uint32(rawFixHeader[24:28])
	fixHeader.checksum = binary.LittleEndian.Uint64(rawFixHeader[28:])

	if fixHeader.checksum != crc64.Checksum(rawFixHeader[:28]) {
		return busMsg{}, fmt.Errorf("bus: fixed header checksum mismatch")
	}

	if fixHeader.partCount > maxPartCount {
		return busMsg{}, fmt.Errorf("bus: too many parts: %d > %d", fixHeader.partCount, maxPartCount)
	}

	if !fixHeader.isVariablePacket() {
		return busMsg{
			parts:     nil,
			fixHeader: fixHeader,
			varHeader: variableHeader{},
		}, nil
	}

	// variableHeader
	varHeader := variableHeader{
		sizes:     make([]uint32, fixHeader.partCount),
		checksums: make([]uint64, fixHeader.partCount),
	}

	rawVarHeader := make([]byte, int((4+8)*fixHeader.partCount+8))
	if _, err := io.ReadFull(c.conn, rawVarHeader); err != nil {
		return busMsg{}, fmt.Errorf("bus: error reading var header: %w", err)
	}

	p := 0
	for i := range varHeader.sizes {
		varHeader.sizes[i] = binary.LittleEndian.Uint32(rawVarHeader[p : p+4])
		p = p + 4
	}
	for i := range varHeader.checksums {
		varHeader.checksums[i] = binary.LittleEndian.Uint64(rawVarHeader[p : p+8])
		p = p + 8
	}
	varHeader.headerChecksum = binary.LittleEndian.Uint64(rawVarHeader[p:])

	if varHeader.headerChecksum != crc64.Checksum(rawVarHeader[:p]) {
		return busMsg{}, fmt.Errorf("bus: variabled header checksum mismatch, expected %x got %x",
			varHeader.headerChecksum, crc64.Checksum(rawVarHeader[:p]))
	}

	parts := make([][]byte, fixHeader.partCount)
	for i, partSize := range varHeader.sizes {
		var part []byte
		if partSize != nullPartSize {
			if partSize > maxPartSize {
				return busMsg{}, fmt.Errorf("bus: part is too big, max %v, actual %v", maxPartSize, partSize)
			}

			part = make([]byte, partSize)
			if _, err := io.ReadFull(c.conn, part); err != nil {
				return busMsg{}, fmt.Errorf("bus: error reading part %d: %w", i, err)
			}
			parts[i] = part
		}

		if varHeader.checksums[i] != crc64.Checksum(part) {
			return busMsg{}, fmt.Errorf("bus: part checksum mismatch")
		}
	}

	return busMsg{
		parts:     parts,
		fixHeader: fixHeader,
		varHeader: varHeader,
	}, nil
}

func (c *Bus) sendHandshake() error {
	handshake := bus.THandshake{
		ConnectionId:   misc.NewProtoFromGUID(c.id),
		EncryptionMode: ptr.Int32(int32(c.options.EncryptionMode)),
	}

	handshakeData, err := proto.Marshal(&handshake)
	if err != nil {
		return fmt.Errorf("bus: error marshal handshake: %w", err)
	}

	data := make([]byte, proto.Size(&handshake)+4)
	binary.LittleEndian.PutUint32(data[0:4], handshakeSignature)
	copy(data[4:], handshakeData)

	handshakePacketID := guid.FromParts(1, 0, 0, 0)
	packet := newMessagePacket(
		handshakePacketID,
		[][]byte{data},
		packetFlagsNone,
		/*computeCRC*/ true)
	l, err := packet.writeTo(c.conn)
	if err != nil {
		return fmt.Errorf("bus: error sending handshake: %w", err)
	}

	c.logger.Debug("Handshake sent",
		log.Any("bytes", l))

	return nil
}

func (c *Bus) receiveHandshake() (bool, error) {
	msg, err := c.receive()
	if err != nil {
		return false, fmt.Errorf("bus: error receiving handshake: %w", err)
	}

	if msg.fixHeader.typ != packetMessage {
		return false, fmt.Errorf("bus: handshake type mismatch")
	}

	if msg.fixHeader.partCount != 1 {
		return false, fmt.Errorf("bus: handshake part count mismatch")
	}

	if msg.fixHeader.packetID != guid.FromParts(1, 0, 0, 0) {
		return false, fmt.Errorf("bus: handshake packet id mismatch")
	}

	data := msg.parts[0]
	if len(data) < 4 {
		return false, fmt.Errorf("bus: handshake data too small")
	}

	signature := binary.LittleEndian.Uint32(data[0:4])
	if signature != handshakeSignature {
		return false, fmt.Errorf("bus: handshake data signature mismatch")
	}

	var handshake bus.THandshake
	if err := proto.Unmarshal(data[4:], &handshake); err != nil {
		return false, fmt.Errorf("bus: handshake data unmarshal error: %w", err)
	}

	em := EncryptionMode(handshake.GetEncryptionMode())
	c.logger.Debug("Handshake received", log.Int("encryption_mode", int(em)))

	if em == EncryptionModeRequired || c.options.EncryptionMode == EncryptionModeRequired {
		if em == EncryptionModeDisabled {
			return false, fmt.Errorf("bus: peer rejects encryption")
		}
		if c.options.EncryptionMode == EncryptionModeDisabled {
			return false, fmt.Errorf("bus: peer requires encryption")
		}
		return true, nil
	}

	return false, nil
}

func (c *Bus) sendSSLAck() error {
	packet := newSSLAckPacket(guid.FromParts(2, 0, 0, 0))
	l, err := packet.writeTo(c.conn)
	if err != nil {
		return fmt.Errorf("bus: error sending SSL ACK: %w", err)
	}

	c.logger.Debug("SSL ACK sent",
		log.Any("bytes", l))

	return nil
}

func (c *Bus) receiveSSLAck() error {
	msg, err := c.receive()
	if err != nil {
		return fmt.Errorf("bus: error receiving SSL ACK: %w", err)
	}

	if msg.fixHeader.typ != packetSSLAck {
		return fmt.Errorf("bus: SSL ACK type mismatch")
	}

	if msg.fixHeader.partCount != 0 {
		return fmt.Errorf("bus: SSL ACK part count mismatch")
	}

	if msg.fixHeader.packetID != guid.FromParts(2, 0, 0, 0) {
		return fmt.Errorf("bus: SSL ACK packet id mismatch")
	}

	c.logger.Debug("SSL ACK received")

	return nil
}

func (c *Bus) establishEncryption(isServer bool) error {
	if err := c.sendHandshake(); err != nil {
		return err
	}

	if encrypt, err := c.receiveHandshake(); err != nil {
		return err
	} else if !encrypt {
		return nil
	}

	if err := c.sendSSLAck(); err != nil {
		return err
	}

	if err := c.receiveSSLAck(); err != nil {
		return err
	}

	var conn *tls.Conn
	if isServer {
		conn = tls.Server(c.conn, c.options.TLSConfig)
	} else {
		conn = tls.Client(c.conn, c.options.TLSConfig)
	}
	if err := conn.Handshake(); err != nil {
		return fmt.Errorf("bus: error performing SSL handshake: %w", err)
	}

	c.conn = conn
	return nil
}

func Dial(ctx context.Context, options Options) (*Bus, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", options.Address)
	if err != nil {
		return nil, err
	}

	bus := NewBus(conn, options)
	if err := bus.establishEncryption(false); err != nil {
		bus.Close()
		return nil, err
	}

	return bus, nil
}
