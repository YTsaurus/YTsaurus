package bus

import (
	"net"
	"testing"
	"time"

	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"

	"encoding/pem"

	"math/big"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
)

func connPair() (a, b net.Conn, err error) {
	var lsn net.Listener
	lsn, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return
	}
	defer func() { _ = lsn.Close() }()

	a, err = net.Dial("tcp", lsn.Addr().String())
	if err != nil {
		return
	}

	b, err = lsn.Accept()
	if err != nil {
		_ = a.Close()
		a = nil
	}
	return
}

// Generate self-signed ECDSA certificate for localhost, same as:
// openssl req -x509 -newkey ec:<(openssl ecparam -name prime256v1)
// -keyout key.pem -out cert.pem -days 1 -noenc -subj "/CN=localhost"
// -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
// -addext "extendedKeyUsage=serverAuth,clientAuth"
func generateSelfSignedECDSA() ([]byte, []byte, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	certTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, certTemplate, certTemplate, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}

func generateSelfSignedRSA() ([]byte, []byte, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	certTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &certTemplate, &certTemplate, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}

	keyDER := x509.MarshalPKCS1PrivateKey(priv)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}

func TestBusPingPong(t *testing.T) {
	a, b, err := connPair()
	require.NoError(t, err)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	busA := NewBus(a, Options{})
	busB := NewBus(b, Options{})

	message := [][]byte{
		[]byte("ping"),
		[]byte("pong"),
		[]byte(""),
		nil,
	}

	err = busA.Send(guid.New(), message, &busSendOptions{})
	require.NoError(t, err)

	transferred, err := busB.Receive()
	require.NoError(t, err)
	require.Equal(t, packetMessage, transferred.fixHeader.typ)
	require.Equal(t, message, transferred.parts)
}

func TestBusEncryptionHandshake(t *testing.T) {
	certPEM, keyPEM, err := generateSelfSignedECDSA()
	require.NoError(t, err)

	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	rootCAs := x509.NewCertPool()
	certOk := rootCAs.AppendCertsFromPEM(certPEM)
	require.True(t, certOk)

	tlsClientConfig := tls.Config{
		ServerName:   "localhost",
		RootCAs:      rootCAs,
		Certificates: []tls.Certificate{certificate},
	}
	tlsServerConfig := tls.Config{
		Certificates: []tls.Certificate{certificate},
	}

	tests := []struct {
		name                   string
		encryptedConn          bool
		handshakeFailure       bool
		clientOpts, serverOpts Options
	}{
		{"Default-Default", false, false, Options{}, Options{}},
		{"Default-Disabled", false, false, Options{}, Options{EncryptionMode: EncryptionModeDisabled}},
		{"Default-Optional", false, false, Options{}, Options{EncryptionMode: EncryptionModeOptional}},
		{"Default-Required", false, true, Options{}, Options{EncryptionMode: EncryptionModeRequired}},
		{"Disabled-Optional", false, false,
			Options{EncryptionMode: EncryptionModeDisabled},
			Options{EncryptionMode: EncryptionModeOptional}},
		{"Disabled-Required", false, true,
			Options{EncryptionMode: EncryptionModeDisabled},
			Options{EncryptionMode: EncryptionModeRequired}},
		{"Optional-Optional", false, false,
			Options{EncryptionMode: EncryptionModeOptional},
			Options{EncryptionMode: EncryptionModeOptional}},
		{"Optional-Required", true, false,
			Options{EncryptionMode: EncryptionModeOptional, TLSConfig: &tlsClientConfig},
			Options{EncryptionMode: EncryptionModeRequired, TLSConfig: &tlsServerConfig}},
		{"Required-Required", true, false,
			Options{EncryptionMode: EncryptionModeRequired, TLSConfig: &tlsClientConfig},
			Options{EncryptionMode: EncryptionModeRequired, TLSConfig: &tlsServerConfig}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a, b, err := connPair()
			require.NoError(t, err)

			defer func() { _ = a.Close() }()
			defer func() { _ = b.Close() }()

			client := NewBus(a, test.clientOpts)
			server := NewBus(b, test.serverOpts)

			err = client.sendHandshake()
			require.NoError(t, err)

			encrypt, err := server.receiveHandshake()
			if test.handshakeFailure {
				require.ErrorContains(t, err, "encryption")
			} else {
				require.NoError(t, err)
				require.Equal(t, encrypt, test.encryptedConn)
			}

			done := make(chan bool)
			go func() {
				defer close(done)
				err := server.establishEncryption(true)
				if test.handshakeFailure {
					require.ErrorContains(t, err, "encryption")
				} else {
					require.NoError(t, err)
					require.Equal(t, encrypt, test.encryptedConn)
				}
			}()

			err = client.establishEncryption(false)
			if test.handshakeFailure {
				require.ErrorContains(t, err, "encryption")
			} else {
				require.NoError(t, err)
				require.Equal(t, encrypt, test.encryptedConn)
			}
			<-done

			if tlsConn, ok := client.conn.(*tls.Conn); ok {
				require.True(t, test.encryptedConn)
				require.True(t, tlsConn.ConnectionState().HandshakeComplete)
			} else {
				require.False(t, test.encryptedConn)
			}

			message := [][]byte{
				[]byte("ping"),
				[]byte("pong"),
				[]byte(""),
				nil,
			}

			err = client.Send(guid.New(), message, &busSendOptions{})
			require.NoError(t, err)

			transferred, err := server.Receive()
			require.NoError(t, err)
			require.Equal(t, packetMessage, transferred.fixHeader.typ)
			require.Equal(t, message, transferred.parts)
		})
	}
}
