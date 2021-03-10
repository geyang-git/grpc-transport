package grpc_transport

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/go-sockaddr"
	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"grpc-transport/internal"
	pb "grpc-transport/proto"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type GrpcTransportConfig struct {
	BindAddrs []string
	Logger    *log.Logger
	BindPort  int
}

type GrpcTransport struct {
	config          *GrpcTransportConfig
	tcpListeners    []net.Listener
	packetCh        chan *memberlist.Packet
	streamCh        chan net.Conn
	connPool        *internal.PacketConnPool
	connEstablished prometheus.Counter
	logger          *log.Logger
	wg              sync.WaitGroup
	shutdown        int32
}

type StreamService struct {
	Send          [][]byte
	Chunk         int
	grpcTransport *GrpcTransport
}

func (s *StreamService) Stream(stream pb.StreamService_StreamServer) error {
	conn := streamConn(stream)
	reader := bufio.NewReader(conn)
	connType, err := reader.ReadString('\n')
	if err != nil {
		s.grpcTransport.logger.Fatalf("failed to read connection type: %v", err)
	}
	connType = strings.Trim(connType, "\n")

	if connType == "p" {
		remoteAddr, err := reader.ReadString('\n')
		if err != nil {
			s.grpcTransport.logger.Fatalf("failed to read remote address: %v", err)
		}

		remoteAddr = strings.Trim(remoteAddr, "\n")

		incoming := true
		if err := s.grpcTransport.connPool.AddAndRead(remoteAddr, conn, incoming); err != nil {
			s.grpcTransport.logger.Fatalf("failed to add connection to pool: %v", err)
		}
	} else {

		s.grpcTransport.streamCh <- conn
		go func() {
			<-stream.Context().Done()
			fmt.Println("lianjie jieshu")
			//s.grpcTransport.logger.Fatalf("*******failed to add connection %v", stream.Context().Err())
		}()
	}
	<-stream.Context().Done()
	return nil
}

func NewGrpcTransport(config *GrpcTransportConfig, reg prometheus.Registerer, impl *StreamService) (*GrpcTransport, error) {
	if len(config.BindAddrs) == 0 {
		return nil, fmt.Errorf("at least one bind address is required")
	}

	var ok bool
	t := GrpcTransport{
		config:   config,
		packetCh: make(chan *memberlist.Packet),
		streamCh: make(chan net.Conn),
		logger:   config.Logger,
	}

	impl.grpcTransport = &t

	t.registerMetrics(reg)

	// Clean up listeners if there's an error.
	defer func() {
		if !ok {
			_ = t.Shutdown()
		}
	}()

	// Build all the TCP listeners.
	port := config.BindPort
	for _, addr := range config.BindAddrs {
		ip := net.ParseIP(addr)

		tcpAddr := &net.TCPAddr{IP: ip, Port: port}
		//tcpLn, err := tls.Listen("tcp", tcpAddr.String(), t.config.TLS)
		tcpLn, err := net.Listen("tcp", tcpAddr.String())
		if err != nil {
			return nil, fmt.Errorf("Failed to start TLS listener on %q port %d: %v", addr, port, err)
		}
		t.tcpListeners = append(t.tcpListeners, tcpLn)
	}

	for i := 0; i < len(config.BindAddrs); i++ {
		t.wg.Add(1)
		go t.grpcListen(t.tcpListeners[i], func(s *grpc.Server) {
			pb.RegisterStreamServiceServer(s, impl)
		})
	}

	t.connPool = internal.NewPacketConnPool(t.packetCh, reg, t.logger, t.tcpListeners[0].Addr().String())

	ok = true
	return &t, nil
}

func (g *GrpcTransport) registerMetrics(reg prometheus.Registerer) {
	g.connEstablished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "memberlist_tls_transport_conn_established",
		Help: "Amount of connections established.",
	})

	reg.MustRegister(g.connEstablished)
}

func (g *GrpcTransport) GetAutoBindPort() int {
	return g.tcpListeners[0].Addr().(*net.TCPAddr).Port
}

func (g *GrpcTransport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	var advertiseAddr net.IP
	var advertisePort int
	if ip != "" {
		// If they've supplied an address, use that.
		advertiseAddr = net.ParseIP(ip)
		if advertiseAddr == nil {
			return nil, 0, fmt.Errorf("Failed to parse advertise address %q", ip)
		}

		// Ensure IPv4 conversion if necessary.
		if ip4 := advertiseAddr.To4(); ip4 != nil {
			advertiseAddr = ip4
		}
		advertisePort = port
	} else {
		if g.config.BindAddrs[0] == "0.0.0.0" {
			// Otherwise, if we're not bound to a specific IP, let's
			// use a suitable private IP address.
			var err error
			ip, err = sockaddr.GetPrivateIP()
			if err != nil {
				return nil, 0, fmt.Errorf("Failed to get interface addresses: %v", err)
			}
			if ip == "" {
				return nil, 0, fmt.Errorf("No private IP address found, and explicit IP not provided")
			}

			advertiseAddr = net.ParseIP(ip)
			if advertiseAddr == nil {
				return nil, 0, fmt.Errorf("Failed to parse advertise address: %q", ip)
			}
		} else {
			// Use the IP that we're bound to, based on the first
			// TCP listener, which we already ensure is there.
			advertiseAddr = g.tcpListeners[0].Addr().(*net.TCPAddr).IP
		}

		// Use the port we are bound to.
		advertisePort = g.GetAutoBindPort()
	}

	return advertiseAddr, advertisePort, nil
}

func (g *GrpcTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	var (
		conn net.Conn
		err  error
		ok   bool
	)

	conn, ok = g.connPool.Get(addr)

	if !ok {
		fmt.Println("Cache miss")
		conn, err = g.dial(addr)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to create new packet connection: %v", err)
		}

		// TODO: We should send a magicbyte signaling the protocol and a version
		// byte first before sending the connection type.
		// Signal that this is a packet connection.
		_, _ = conn.Write([]byte{'p', '\n'})
		// TODO: This might only be the private, not the public address. We should
		// probably send the advertise address down the wire.
		_, _ = conn.Write(append([]byte(g.tcpListeners[0].Addr().String()), '\n'))

		incoming := false
		_ = g.connPool.AddAndRead(addr, conn, incoming)
	}

	// TODO: This is probably not performing very well. How about prefixing each msg
	// with a length and reading just as far as the length?
	msg := []byte(base64.StdEncoding.EncodeToString(b))
	msg = append(msg, '\n')

	// This connection might be shared among multiple goroutines. conn.Write is
	// thread safe. Make sure to write in one go so no concurrent write gets in
	// between.
	_, err = conn.Write(msg)
	if err != nil {
		//g.logger.Println(err)
		return time.Time{}, err
	}

	return time.Now(), nil

}

func (g *GrpcTransport) PacketCh() <-chan *memberlist.Packet {
	return g.packetCh
}

func (g *GrpcTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := g.dial(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream connection: %v", err)
	}

	// Signal that this is a stream connection.
	_, err = conn.Write([]byte{'s', '\n'})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (g *GrpcTransport) StreamCh() <-chan net.Conn {
	return g.streamCh
}

func (g *GrpcTransport) Shutdown() error {
	// This will avoid log spam about errors when we shut down.
	atomic.StoreInt32(&g.shutdown, 1)

	// Rip through all the connections and shut them down.
	for _, listener := range g.tcpListeners {
		listener.Close()
	}

	// Block until all the listener threads have died.
	g.wg.Wait()

	g.connPool.Shutdown()
	return nil
}

func (g *GrpcTransport) dial(addr string) (net.Conn, error) {
	grpcConnObj, err := grpc.Dial(
		addr,
		grpc.WithBlock(),
		grpc.WithInsecure())
	if err != nil {
		//t.Fatalf("err: %s", err)
	}
	resp, _ := pb.NewStreamServiceClient(grpcConnObj).Stream(context.Background())
	conn := streamConn(resp)
	// StreamService_StreamClient->Conn
	return conn, nil
}

func (g *GrpcTransport) grpcListen(lis net.Listener, register func(*grpc.Server)) {
	defer g.wg.Done()
	server := grpc.NewServer()
	register(server)
	_ = server.Serve(lis)
}

func streamConn(stream grpc.Stream) *Conn {
	dataFieldFunc := func(msg proto.Message) *[]byte {
		return &msg.(*pb.Bytes).Data
	}

	return &Conn{
		Stream:   stream,
		Request:  &pb.Bytes{},
		Response: &pb.Bytes{},
		Encode:   SimpleEncoder(dataFieldFunc),
		Decode:   SimpleDecoder(dataFieldFunc),
	}
}
