package simpleradio

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/dharmab/skyeye/pkg/coalitions"
	"github.com/dharmab/skyeye/pkg/simpleradio/types"
	"github.com/dharmab/skyeye/pkg/simpleradio/voice"
	"gopkg.in/hraban/opus.v2"
)

// The streaming format is mono, 16 kHz, with exactly 40 ms per packet. PCM is
// normalized float32 ([-1,1]); Opus contains one SRS-compatible 40 ms packet.
const (
	StreamSampleRate    = 16000
	StreamFrameSamples  = 640
	StreamFrameDuration = 40 * time.Millisecond
)

var (
	ErrStreamDisconnected   = errors.New("SRS stream disconnected")
	ErrStreamQueueFull      = errors.New("SRS stream queue full")
	ErrStreamBusy           = errors.New("SRS stream transmitter busy")
	ErrStreamUnderrun       = errors.New("SRS stream producer underrun")
	ErrStreamFrame          = errors.New("SRS stream requires one mono 16 kHz 40 ms PCM or Opus frame")
	ErrStreamAuthentication = errors.New("SRS stream authentication failed")
	ErrStreamConfiguration  = errors.New("invalid SRS stream configuration")
)

// StreamFrame owns its buffers. Transmit accepts exactly one of PCM/Opus. Sending
// a frame transfers ownership to Transmit: the producer must not reuse its slices.
// Received frames contain both encodings and belong exclusively to the consumer.
type StreamFrame struct {
	PCM  []float32
	Opus []byte
}

type StreamEventKind string

const (
	StreamAudio StreamEventKind = "audio"
	StreamGap   StreamEventKind = "gap"
)

// StreamMetadata preserves wire identity independently of display names. Coalition
// is unavailable on UDP and is known only after a matching TCP peer update. An
// external-AWACS unit sentinel or zero is never advertised as an aircraft binding.
type StreamMetadata struct {
	OriginGUID          types.GUID
	RelayGUID           types.GUID
	UnitID              uint32
	AircraftUnitIDKnown bool
	PacketID            uint64
	Hops                byte
	Frequencies         []voice.Frequency
	Coalition           coalitions.Coalition
	CoalitionKnown      bool
}

type StreamEvent struct {
	Kind           StreamEventKind
	TransmissionID uint64
	Metadata       StreamMetadata
	Frame          StreamFrame
	// LostPackets counts skipped sequence numbers; no invented audio or FEC is used.
	LostPackets uint64
}

// StreamTimer/StreamClock allow deterministic gap, timeout and pacing fixtures.
type StreamTimer interface {
	C() <-chan time.Time
	Stop() bool
}
type StreamClock interface {
	Now() time.Time
	NewTimer(time.Duration) StreamTimer
}
type wallStreamClock struct{}
type wallStreamTimer struct{ *time.Timer }

func (wallStreamClock) Now() time.Time { return time.Now() }
func (wallStreamClock) NewTimer(d time.Duration) StreamTimer {
	return wallStreamTimer{time.NewTimer(d)}
}
func (t wallStreamTimer) C() <-chan time.Time { return t.Timer.C }

// StreamOptions bounds all audio and peer state. Zero values select defaults.
// Dial must honor ctx and return a connected stream/datagram net.Conn; Close must
// interrupt Read and Write. The production default uses net.Dialer.DialContext.
type StreamOptions struct {
	Gap          time.Duration
	Underrun     time.Duration
	ReceiveQueue int
	PacketQueue  int
	MaxSenders   int
	MaxPeers     int
	Clock        StreamClock
	Dial         func(context.Context, string, string) (net.Conn, error)
}

func (o StreamOptions) normalized() (StreamOptions, error) {
	if o.Gap == 0 {
		o.Gap = 300 * time.Millisecond
	}
	if o.Underrun == 0 {
		o.Underrun = 300 * time.Millisecond
	}
	if o.ReceiveQueue == 0 {
		o.ReceiveQueue = 64
	}
	if o.PacketQueue == 0 {
		o.PacketQueue = 64
	}
	if o.MaxSenders == 0 {
		o.MaxSenders = 128
	}
	if o.MaxPeers == 0 {
		o.MaxPeers = 1024
	}
	if o.Gap < StreamFrameDuration || o.Gap > 10*time.Second || o.Underrun < StreamFrameDuration || o.Underrun > 10*time.Second || o.ReceiveQueue < 1 || o.ReceiveQueue > 4096 || o.PacketQueue < 1 || o.PacketQueue > 4096 || o.MaxSenders < 1 || o.MaxSenders > 4096 || o.MaxPeers < 1 || o.MaxPeers > 65536 {
		return o, ErrStreamConfiguration
	}
	if o.Clock == nil {
		o.Clock = wallStreamClock{}
	}
	if o.Dial == nil {
		o.Dial = (&net.Dialer{}).DialContext
	}
	return o, nil
}

type streamSender struct {
	last     uint64
	haveLast bool
	deadline time.Time
	active   bool
	id       uint64
	metadata StreamMetadata
	decoder  *opus.Decoder
}

// Stream is one explicit connection epoch. It never reconnects automatically or
// replays queued audio. Close it before connecting a replacement; every connection
// gets a fresh GUID and empty sender/peer/codec state. Legacy Client is unchanged.
type Stream struct {
	config       types.ClientConfiguration // credential is erased before storage
	opts         StreamOptions
	tcp, udp     net.Conn
	ctx          context.Context
	cancel       context.CancelFunc
	rx           chan StreamEvent
	packets      chan []byte
	done         chan struct{}
	auth         chan error
	wg           sync.WaitGroup
	stopOnce     sync.Once
	mu           sync.Mutex
	err          error
	peers        map[types.GUID]coalitions.Coalition
	secure       bool
	tx           chan struct{}
	tcpWrite     sync.Mutex
	udpWrite     chan struct{}
	sequence     uint64
	nextTransmit time.Time
}

// ConnectStream is the only constructor that opens sockets. It performs bounded
// TCP/UDP dialing and waits for External AWACS authentication. Credentials are
// used only for that handshake; no protocol payloads or network errors are logged
// or returned, since either can contain echoed secret values.
func ConnectStream(ctx context.Context, config types.ClientConfiguration, options StreamOptions) (*Stream, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	o, err := options.normalized()
	if err != nil {
		return nil, err
	}
	if config.Address == "" || config.ClientName == "" || len(config.Radios) == 0 || len(config.Radios) > 16 || (config.Coalition != coalitions.Blue && config.Coalition != coalitions.Red) {
		return nil, ErrStreamConfiguration
	}
	for _, r := range config.Radios {
		if (r.IsEncrypted && r.EncryptionKey == 0) || r.GuardFrequency != 0 || r.ShouldRetransmit || math.IsNaN(r.Frequency) || math.IsInf(r.Frequency, 0) || r.Frequency <= 0 || r.Frequency > 1<<53 || math.Trunc(r.Frequency) != r.Frequency || (r.Modulation != types.ModulationAM && r.Modulation != types.ModulationFM) {
			return nil, ErrStreamConfiguration
		}
	}
	if config.ConnectionTimeout < 0 {
		return nil, ErrStreamConfiguration
	}
	if config.ConnectionTimeout == 0 {
		config.ConnectionTimeout = 5 * time.Second
	}
	if config.ConnectionTimeout > time.Minute {
		return nil, ErrStreamConfiguration
	}
	dialCtx, cancelDial := context.WithTimeout(ctx, config.ConnectionTimeout)
	defer cancelDial()
	tcp, err := o.Dial(dialCtx, "tcp", config.Address)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrStreamDisconnected
	}
	udp, err := o.Dial(dialCtx, "udp", config.Address)
	if err != nil {
		tcp.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrStreamDisconnected
	}
	password := config.ExternalAWACSModePassword
	config.ExternalAWACSModePassword = ""
	config.GUID = string(types.NewGUID())
	config.Radios = append([]types.Radio(nil), config.Radios...)
	sessionCtx, cancel := context.WithCancel(ctx)
	s := &Stream{config: config, opts: o, tcp: tcp, udp: udp, ctx: sessionCtx, cancel: cancel, rx: make(chan StreamEvent, o.ReceiveQueue), packets: make(chan []byte, o.PacketQueue), done: make(chan struct{}), auth: make(chan error, 1), peers: make(map[types.GUID]coalitions.Coalition), tx: make(chan struct{}, 1), sequence: 1, secure: true, udpWrite: make(chan struct{}, 1)}
	// Closing sockets interrupts pending network I/O, including the initial writes.
	stopInit := context.AfterFunc(dialCtx, func() { s.stop(ErrStreamDisconnected) })
	defer stopInit()
	if s.sendMessage(types.MessageSync, "") != nil || s.sendMessage(types.MessageExternalAWACSModePassword, password) != nil {
		s.stop(ErrStreamDisconnected)
		return nil, ErrStreamDisconnected
	}
	password = ""
	s.wg.Add(4)
	go func() { defer s.wg.Done(); s.readTCP() }()
	go func() { defer s.wg.Done(); s.readUDP() }()
	go func() { defer s.wg.Done(); s.dispatch() }()
	go func() { defer s.wg.Done(); s.pings() }()
	go func() { <-sessionCtx.Done(); s.stop(context.Canceled) }()
	go func() { s.wg.Wait(); close(s.done) }()
	authTimer := o.Clock.NewTimer(config.ConnectionTimeout)
	defer authTimer.Stop()
	select {
	case err := <-s.auth:
		if dialCtx.Err() != nil {
			s.stop(ErrStreamDisconnected)
			<-s.done
			return nil, ErrStreamDisconnected
		}
		if err != nil {
			s.stop(err)
			<-s.done
			return nil, err
		}
		return s, nil
	case <-authTimer.C():
		s.stop(ErrStreamAuthentication)
	case <-dialCtx.Done():
		s.stop(ErrStreamDisconnected)
	case <-s.ctx.Done():
	}
	<-s.done
	return nil, s.Err()
}

func (s *Stream) info() types.ClientInfo {
	return types.ClientInfo{GUID: types.GUID(s.config.GUID), Name: s.config.ClientName, Coalition: s.config.Coalition, AllowRecording: s.config.AllowRecording, RadioInfo: types.RadioInfo{UnitID: 100000002, Unit: "External AWACS", Radios: s.config.Radios, IFF: types.NewIFF(), Ambient: types.NewAmbient()}, Position: &types.Position{}}
}
func (s *Stream) sendMessage(kind types.MessageType, password string) error {
	info := s.info()
	b, err := json.Marshal(types.Message{Version: "2.1.0.2", Type: kind, Client: &info, ExternalAWACSModePassword: password})
	if err != nil {
		return ErrStreamDisconnected
	}
	b = append(b, '\n')
	s.tcpWrite.Lock()
	defer s.tcpWrite.Unlock()
	s.tcp.SetWriteDeadline(time.Now().Add(s.config.ConnectionTimeout))
	for len(b) > 0 {
		n, e := s.tcp.Write(b)
		if e != nil || n == 0 {
			return ErrStreamDisconnected
		}
		b = b[n:]
	}
	return nil
}
func (s *Stream) writeUDP(b []byte) error { return s.writeUDPContext(s.ctx, b) }

func (s *Stream) writeUDPContext(ctx context.Context, b []byte) error {
	select {
	case s.udpWrite <- struct{}{}:
		defer func() { <-s.udpWrite }()
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.Err()
	}
	s.udp.SetWriteDeadline(time.Now().Add(s.config.ConnectionTimeout))
	interrupted := make(chan struct{})
	stopInterrupt := context.AfterFunc(ctx, func() { s.udp.SetWriteDeadline(time.Now()); close(interrupted) })
	defer func() {
		if !stopInterrupt() {
			<-interrupted
		}
	}()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	n, e := s.udp.Write(b)
	if e != nil || n != len(b) {
		return ErrStreamDisconnected
	}
	return nil
}
func (s *Stream) stop(err error) {
	s.stopOnce.Do(func() {
		if errors.Is(err, ErrStreamDisconnected) && s.ctx.Err() != nil {
			err = s.ctx.Err()
		}
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		s.cancel()
		s.tcp.Close()
		s.udp.Close()
	})
}
func (s *Stream) Receive() <-chan StreamEvent { return s.rx }
func (s *Stream) Done() <-chan struct{}       { return s.done }
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil && s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	return s.err
}

// Close is idempotent, interrupts all I/O and waits for readers to stop. Callers
// should use Err to distinguish cancellation/close from a network disconnection.
func (s *Stream) Close() error { s.stop(context.Canceled); <-s.done; return nil }
func (s *Stream) authenticate(err error) {
	select {
	case s.auth <- err:
	default:
	}
}

func (s *Stream) readTCP() {
	scanner := bufio.NewScanner(s.tcp)
	scanner.Buffer(make([]byte, 4096), 256*1024)
	for scanner.Scan() {
		var m types.Message
		if json.Unmarshal(scanner.Bytes(), &m) != nil {
			s.stop(ErrStreamDisconnected)
			return
		}
		if m.Type == types.MessageVersionMismatch || m.Type == types.MessageExternalAWACSModeDisconnect {
			s.authenticate(ErrStreamAuthentication)
			s.stop(ErrStreamAuthentication)
			return
		}
		if m.Type == types.MessageExternalAWACSModePassword {
			if m.Client == nil || m.Client.Coalition != s.config.Coalition {
				s.authenticate(ErrStreamAuthentication)
				s.stop(ErrStreamAuthentication)
				return
			}
		}
		s.mu.Lock()
		if value, ok := m.ServerSettings[string(types.CoalitionAudioSecurity)]; ok {
			s.secure = !strings.EqualFold(strings.TrimSpace(value), "false")
		}
		if value, ok := m.ServerSettings[string(types.ExternalAWACSMode)]; ok && strings.EqualFold(strings.TrimSpace(value), "false") {
			s.mu.Unlock()
			s.authenticate(ErrStreamAuthentication)
			s.stop(ErrStreamAuthentication)
			return
		}
		switch m.Type {
		case types.MessageSync:
			if len(m.Clients) > s.opts.MaxPeers {
				s.mu.Unlock()
				s.stop(ErrStreamQueueFull)
				return
			}
			s.peers = make(map[types.GUID]coalitions.Coalition, len(m.Clients))
			for _, p := range m.Clients {
				if len(p.GUID) != types.GUIDLength {
					continue
				}
				s.peers[p.GUID] = p.Coalition
			}
		case types.MessageUpdate, types.MessageRadioUpdate:
			if m.Client != nil && len(m.Client.GUID) == types.GUIDLength {
				if _, ok := s.peers[m.Client.GUID]; !ok && len(s.peers) >= s.opts.MaxPeers {
					s.mu.Unlock()
					s.stop(ErrStreamQueueFull)
					return
				}
				s.peers[m.Client.GUID] = m.Client.Coalition
			}
		case types.MessageClientDisconnect:
			if m.Client != nil {
				delete(s.peers, m.Client.GUID)
			}
		}
		s.mu.Unlock()
		if m.Type == types.MessageExternalAWACSModePassword {
			if s.sendMessage(types.MessageRadioUpdate, "") != nil {
				s.stop(ErrStreamDisconnected)
				return
			}
			s.authenticate(nil)
		}
	}
	s.stop(ErrStreamDisconnected)
}
func (s *Stream) readUDP() {
	buffer := make([]byte, 1500)
	for {
		n, err := s.udp.Read(buffer)
		if err != nil {
			s.stop(ErrStreamDisconnected)
			return
		}
		if n == types.GUIDLength {
			continue
		}
		if n <= types.GUIDLength || n == len(buffer) {
			continue
		}
		b := append([]byte(nil), buffer[:n]...)
		select {
		case s.packets <- b:
		case <-s.ctx.Done():
			return
		default:
			s.stop(ErrStreamQueueFull)
			return
		}
	}
}
func (s *Stream) pings() {
	for {
		if s.sendMessage(types.MessagePing, "") != nil || s.writeUDP([]byte(s.config.GUID)) != nil {
			s.stop(ErrStreamDisconnected)
			return
		}
		timer := s.opts.Clock.NewTimer(15 * time.Second)
		select {
		case <-timer.C():
		case <-s.ctx.Done():
			timer.Stop()
			return
		}
	}
}
func (s *Stream) emit(e StreamEvent) bool {
	select {
	case <-s.ctx.Done():
		return false
	default:
	}
	select {
	case s.rx <- e:
		return true
	default:
		s.stop(ErrStreamQueueFull)
		return false
	}
}

func (s *Stream) dispatch() {
	defer func() {
		for {
			select {
			case <-s.rx:
			default:
				close(s.rx)
				return
			}
		}
	}()
	// High watermarks survive gap completion for this entire connection epoch.
	// On reaching MaxSenders, terminate the connection visibly rather than
	// evicting tombstones and admitting stale relay packets again.
	senders := make(map[types.GUID]*streamSender)
	var nextID uint64
	timer := s.opts.Clock.NewTimer(StreamFrameDuration)
	defer func() { timer.Stop() }()
	for {
		select {
		case <-s.ctx.Done():
			// Never deliver buffered audio from a dead epoch to a replacement runtime.
			for {
				select {
				case <-s.rx:
				default:
					return
				}
			}
		case <-timer.C():
			now := s.opts.Clock.Now()
			for _, state := range senders {
				if state.active && !now.Before(state.deadline) {
					state.active = false
					state.decoder = nil
					if !s.emit(StreamEvent{Kind: StreamGap, TransmissionID: state.id, Metadata: copyStreamMetadata(state.metadata)}) {
						return
					}
				}
			}
			timer = s.opts.Clock.NewTimer(StreamFrameDuration)
		case b := <-s.packets:
			packet, err := voice.Decode(b)
			if err != nil || len(packet.AudioBytes) == 0 {
				continue
			}
			origin := types.GUID(packet.OriginGUID)
			if origin == types.GUID(s.config.GUID) {
				continue
			}
			matched := false
			validRoutes := len(packet.Frequencies) > 0
			for _, f := range packet.Frequencies {
				if math.IsNaN(f.Frequency) || math.IsInf(f.Frequency, 0) || f.Frequency <= 0 || f.Frequency > 1<<53 || math.Trunc(f.Frequency) != f.Frequency {
					validRoutes = false
				}
			}
			if !validRoutes {
				continue
			}
			for _, f := range packet.Frequencies {
				for _, r := range s.config.Radios {
					encryption := byte(0)
					if r.IsEncrypted {
						encryption = r.EncryptionKey
					}
					if f.Frequency == r.Frequency && f.Modulation == byte(r.Modulation) && f.Encryption == encryption {
						matched = true
					}
				}
			}
			if !matched {
				continue
			}
			s.mu.Lock()
			peer, known := s.peers[origin]
			secure := s.secure
			s.mu.Unlock()
			if secure && (!known || peer != s.config.Coalition) {
				continue
			}
			state := senders[origin]
			if state == nil {
				if len(senders) >= s.opts.MaxSenders {
					s.stop(ErrStreamQueueFull)
					return
				}
				state = &streamSender{}
				senders[origin] = state
			}
			if state.haveLast && packet.PacketID <= state.last {
				continue
			}
			lost := uint64(0)
			if state.haveLast && state.active && s.opts.Clock.Now().Before(state.deadline) {
				lost = packet.PacketID - state.last - 1
			}
			state.last = packet.PacketID
			state.haveLast = true
			now := s.opts.Clock.Now()
			if state.active && !now.Before(state.deadline) {
				state.active = false
				state.decoder = nil
				if !s.emit(StreamEvent{Kind: StreamGap, TransmissionID: state.id, Metadata: copyStreamMetadata(state.metadata)}) {
					return
				}
			}
			if !state.active {
				state.decoder, err = opus.NewDecoder(StreamSampleRate, 1)
				if err != nil {
					s.stop(ErrStreamFrame)
					return
				}

			}
			pcm := make([]float32, StreamFrameSamples*3)
			n, err := state.decoder.DecodeFloat32(packet.AudioBytes, pcm)
			if err != nil || n != StreamFrameSamples {
				continue
			}
			if !state.active {
				nextID++
				state.id = nextID
				state.active = true
			}
			state.deadline = now.Add(s.opts.Gap)
			metadata := StreamMetadata{OriginGUID: origin, RelayGUID: types.GUID(packet.RelayGUID), UnitID: packet.UnitID, AircraftUnitIDKnown: packet.UnitID != 0 && packet.UnitID != 100000002, PacketID: packet.PacketID, Hops: packet.Hops, Frequencies: append([]voice.Frequency(nil), packet.Frequencies...), Coalition: peer, CoalitionKnown: known}
			state.metadata = metadata
			if !s.emit(StreamEvent{Kind: StreamAudio, TransmissionID: state.id, Metadata: copyStreamMetadata(metadata), Frame: StreamFrame{PCM: pcm[:n:n], Opus: append([]byte(nil), packet.AudioBytes...)}, LostPackets: lost}) {
				return
			}
		}
	}
}

// Transmit consumes frames as they arrive; it sends the first immediately and
// spaces subsequent packets by at least 40 ms. It buffers no complete utterance.
// The input channel provides bounded producer backpressure (an unbuffered channel
// is recommended). Only one producer may transmit per connection. A producer gap
// greater than Underrun aborts the utterance; late frames are never replayed.
func (s *Stream) Transmit(ctx context.Context, frames <-chan StreamFrame) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.ctx.Err() != nil {
		return s.Err()
	}
	select {
	case s.tx <- struct{}{}:
		defer func() { <-s.tx }()
	default:
		return ErrStreamBusy
	}
	if s.config.Mute {
		return errors.New("SRS stream transmission muted")
	}
	encoder, err := opus.NewEncoder(StreamSampleRate, 1, opusApplicationVoIP)
	if err != nil {
		return ErrStreamFrame
	}
	validator, err := opus.NewDecoder(StreamSampleRate, 1)
	if err != nil {
		return ErrStreamFrame
	}
	frequencies := make([]voice.Frequency, 0, len(s.config.Radios))
	for _, r := range s.config.Radios {
		encryption := byte(0)
		if r.IsEncrypted {
			encryption = r.EncryptionKey
		}
		frequencies = append(frequencies, voice.Frequency{Frequency: r.Frequency, Modulation: byte(r.Modulation), Encryption: encryption})
	}
	next := s.nextTransmit
	for {
		timer := s.opts.Clock.NewTimer(s.opts.Underrun)
		var frame StreamFrame
		select {
		case f, ok := <-frames:
			timer.Stop()
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if s.ctx.Err() != nil {
					return s.Err()
				}
				return nil
			}
			frame = f
		case <-timer.C():
			return ErrStreamUnderrun
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-s.ctx.Done():
			timer.Stop()
			return s.Err()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if s.ctx.Err() != nil {
			return s.Err()
		}
		var audio []byte
		if len(frame.PCM) > 0 && len(frame.Opus) == 0 {
			if len(frame.PCM) != StreamFrameSamples {
				return ErrStreamFrame
			}
			for _, v := range frame.PCM {
				if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) || v < -1 || v > 1 {
					return ErrStreamFrame
				}
			}
			audio = make([]byte, 1024)
			n, e := encoder.EncodeFloat32(frame.PCM, audio)
			if e != nil {
				return ErrStreamFrame
			}
			audio = audio[:n]
		} else if len(frame.Opus) > 0 && len(frame.PCM) == 0 && len(frame.Opus) <= 1024 {
			n, e := validator.DecodeFloat32(frame.Opus, make([]float32, StreamFrameSamples*3))
			if e != nil || n != StreamFrameSamples {
				return ErrStreamFrame
			}
			audio = append([]byte(nil), frame.Opus...)
		} else {
			return ErrStreamFrame
		}
		if delay := next.Sub(s.opts.Clock.Now()); delay > 0 {
			pace := s.opts.Clock.NewTimer(delay)
			select {
			case <-pace.C():
			case <-ctx.Done():
				pace.Stop()
				return ctx.Err()
			case <-s.ctx.Done():
				pace.Stop()
				return s.Err()
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if s.ctx.Err() != nil {
			return s.Err()
		}
		guid := []byte(s.config.GUID)
		packet := voice.NewPacket(audio, frequencies, 100000002, s.sequence, 0, guid, guid)
		s.sequence++
		if s.writeUDPContext(ctx, packet.Encode()) != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.stop(ErrStreamDisconnected)
			return ErrStreamDisconnected
		}
		next = s.opts.Clock.Now().Add(StreamFrameDuration)
		s.nextTransmit = next
	}
}

var _ io.Closer = (*Stream)(nil)

func copyStreamMetadata(m StreamMetadata) StreamMetadata {
	m.Frequencies = append([]voice.Frequency(nil), m.Frequencies...)
	return m
}
