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
	"testing"
	"time"

	"github.com/dharmab/skyeye/pkg/coalitions"
	"github.com/dharmab/skyeye/pkg/simpleradio/types"
	"github.com/dharmab/skyeye/pkg/simpleradio/voice"
	"gopkg.in/hraban/opus.v2"
)

// manualStreamClock drives protocol timers without wall-clock sleeps. Real
// deadlines below are test-failure guards, never the source of packet timing.
type manualStreamClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  map[*manualStreamTimer]bool
	changed chan struct{}
}
type manualStreamTimer struct {
	clock    *manualStreamClock
	deadline time.Time
	ch       chan time.Time
}

func newManualStreamClock() *manualStreamClock {
	return &manualStreamClock{now: time.Unix(1_700_000_000, 0), timers: make(map[*manualStreamTimer]bool), changed: make(chan struct{}, 1)}
}
func (c *manualStreamClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *manualStreamClock) notify() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}
func (c *manualStreamClock) NewTimer(d time.Duration) StreamTimer {
	c.mu.Lock()
	timer := &manualStreamTimer{clock: c, deadline: c.now.Add(d), ch: make(chan time.Time, 1)}
	if d <= 0 {
		timer.ch <- c.now
	} else {
		c.timers[timer] = true
	}
	c.mu.Unlock()
	c.notify()
	return timer
}
func (t *manualStreamTimer) C() <-chan time.Time { return t.ch }
func (t *manualStreamTimer) Stop() bool {
	t.clock.mu.Lock()
	_, ok := t.clock.timers[t]
	delete(t.clock.timers, t)
	t.clock.mu.Unlock()
	t.clock.notify()
	return ok
}
func (c *manualStreamClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	for timer := range c.timers {
		if !timer.deadline.After(c.now) {
			delete(c.timers, timer)
			timer.ch <- c.now
		}
	}
	c.mu.Unlock()
	c.notify()
}
func (c *manualStreamClock) awaitDeadline(t *testing.T, deadline time.Time) {
	c.awaitDeadlineCount(t, deadline, 1)
}
func (c *manualStreamClock) awaitDeadlineCount(t *testing.T, deadline time.Time, count int) {
	t.Helper()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		c.mu.Lock()
		found := 0
		for timer := range c.timers {
			if timer.deadline.Equal(deadline) {
				found++
			}
		}
		c.mu.Unlock()
		if found >= count {
			return
		}
		select {
		case <-c.changed:
		case <-timeout.C:
			t.Fatalf("clock timer for %v was not registered", deadline)
		}
	}
}

type streamFixtureAddr string

func (a streamFixtureAddr) Network() string { return string(a) }
func (a streamFixtureAddr) String() string  { return "fixture" }

type streamDatagram struct {
	incoming chan []byte
	outgoing chan []byte
	closed   chan struct{}
	once     sync.Once
}

func newStreamDatagram() *streamDatagram {
	return &streamDatagram{incoming: make(chan []byte, 256), outgoing: make(chan []byte, 256), closed: make(chan struct{})}
}
func (d *streamDatagram) Read(b []byte) (int, error) {
	select {
	case p := <-d.incoming:
		return copy(b, p), nil
	case <-d.closed:
		return 0, net.ErrClosed
	}
}
func (d *streamDatagram) Write(b []byte) (int, error) {
	p := append([]byte(nil), b...)
	select {
	case <-d.closed:
		return 0, net.ErrClosed
	default:
	}
	select {
	case d.outgoing <- p:
		return len(b), nil
	case <-d.closed:
		return 0, net.ErrClosed
	}
}
func (d *streamDatagram) Close() error                     { d.once.Do(func() { close(d.closed) }); return nil }
func (d *streamDatagram) LocalAddr() net.Addr              { return streamFixtureAddr("udp") }
func (d *streamDatagram) RemoteAddr() net.Addr             { return streamFixtureAddr("udp") }
func (d *streamDatagram) SetDeadline(time.Time) error      { return nil }
func (d *streamDatagram) SetReadDeadline(time.Time) error  { return nil }
func (d *streamDatagram) SetWriteDeadline(time.Time) error { return nil }

type streamingFixture struct {
	stream   *Stream
	clock    *manualStreamClock
	udp      *streamDatagram
	server   net.Conn
	messages chan types.Message
	self     types.GUID
}

func newStreamingFixture(t *testing.T, change func(*StreamOptions)) *streamingFixture {
	t.Helper()
	f := &streamingFixture{clock: newManualStreamClock(), udp: newStreamDatagram(), messages: make(chan types.Message, 64)}
	client, server := net.Pipe()
	f.server = server
	opts := StreamOptions{Clock: f.clock, Gap: 300 * time.Millisecond, Underrun: 200 * time.Millisecond, ReceiveQueue: 32, PacketQueue: 32, MaxSenders: 8, MaxPeers: 8, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		switch network {
		case "tcp":
			return client, nil
		case "udp":
			return f.udp, nil
		default:
			return nil, errors.New("unexpected network")
		}
	}}
	if change != nil {
		change(&opts)
	}
	go func() {
		scanner := bufio.NewScanner(server)
		for scanner.Scan() {
			var m types.Message
			if json.Unmarshal(scanner.Bytes(), &m) != nil {
				continue
			}
			f.messages <- m
			if m.Type == types.MessageExternalAWACSModePassword {
				_ = json.NewEncoder(server).Encode(types.Message{Type: types.MessageServerSettings, ServerSettings: map[string]string{string(types.CoalitionAudioSecurity): "false"}})
				ack := types.Message{Type: types.MessageExternalAWACSModePassword, Client: &types.ClientInfo{Coalition: coalitions.Blue}}
				_ = json.NewEncoder(server).Encode(ack)
			}
		}
	}()
	cfg := types.ClientConfiguration{Address: "fixture.invalid:5002", ClientName: "fixture [BOT]", Coalition: coalitions.Blue, ExternalAWACSModePassword: "fixture-password", Radios: []types.Radio{{Frequency: 251000000, Modulation: types.ModulationAM}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	s, err := ConnectStream(ctx, cfg, opts)
	if err != nil {
		t.Fatalf("ConnectStream: %v", err)
	}
	f.stream = s
	// The first sync message records the fresh session's own GUID.
	select {
	case m := <-f.messages:
		if m.Client != nil {
			f.self = m.Client.GUID
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing sync")
	}
	t.Cleanup(func() { _ = s.Close(); _ = server.Close() })
	return f
}
func (f *streamingFixture) inject(packet voice.Packet) { f.udp.incoming <- packet.Encode() }
func streamFixtureAudio(t *testing.T) []byte {
	t.Helper()
	enc, err := opus.NewEncoder(16000, 1, opusApplicationVoIP)
	if err != nil {
		t.Fatal(err)
	}
	b, err := encodeFrame(enc, testTone(640))
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func streamFixturePacket(audio []byte, sender types.GUID, seq uint64) voice.Packet {
	return voice.NewPacket(audio, []voice.Frequency{{Frequency: 251000000, Modulation: byte(types.ModulationAM)}}, 42, seq, 0, []byte(sender), []byte(sender))
}
func nextStreamEvent(t *testing.T, s *Stream) StreamEvent {
	t.Helper()
	select {
	case e, ok := <-s.Receive():
		if !ok {
			t.Fatalf("stream closed: %v", s.Err())
		}
		return e
	case <-time.After(3 * time.Second):
		t.Fatal("missing stream event")
		return StreamEvent{}
	}
}
func nextStreamVoice(t *testing.T, d *streamDatagram) *voice.Packet {
	t.Helper()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case b := <-d.outgoing:
			if len(b) <= types.GUIDLength {
				continue
			}
			p, err := voice.Decode(b)
			if err != nil {
				t.Fatal(err)
			}
			return p
		case <-timeout.C:
			t.Fatal("missing outbound voice")
			return nil
		}
	}
}
func assertNoStreamEvent(t *testing.T, s *Stream) {
	t.Helper()
	select {
	case e := <-s.Receive():
		t.Fatalf("unexpected event: %+v", e)
	default:
	}
}

func TestStreamIncrementalReceivePreservesMetadataAndOverlap(t *testing.T) {
	f := newStreamingFixture(t, nil)
	audio := streamFixtureAudio(t)
	alice := types.GUID("1111111111111111111111")
	bob := types.GUID("2222222222222222222222")
	p := streamFixturePacket(audio, alice, 10)
	p.RelayGUID = []byte("3333333333333333333333")
	p.Hops = 2
	p = voice.NewPacket(p.AudioBytes, append(p.Frequencies, voice.Frequency{Frequency: 252000000, Modulation: byte(types.ModulationFM), Encryption: 7}), p.UnitID, p.PacketID, p.Hops, p.RelayGUID, p.OriginGUID)
	f.inject(p)
	first := nextStreamEvent(t, f.stream)
	if first.Kind != StreamAudio || first.Metadata.OriginGUID != alice || first.Metadata.RelayGUID != types.GUID(p.RelayGUID) || first.Metadata.PacketID != 10 || first.Metadata.UnitID != 42 || !first.Metadata.AircraftUnitIDKnown || first.Metadata.Hops != 2 {
		t.Fatalf("metadata was not preserved: %+v", first)
	}
	if first.Metadata.CoalitionKnown {
		t.Fatal("UDP cannot establish sender coalition")
	}
	if len(first.Metadata.Frequencies) != 2 || first.Metadata.Frequencies[1].Encryption != 7 {
		t.Fatalf("original routes lost: %+v", first.Metadata.Frequencies)
	}
	if len(first.Frame.PCM) != 640 || len(first.Frame.Opus) == 0 {
		t.Fatal("frame must expose explicit decoded PCM and original Opus")
	}
	// Neither speaker has stopped: each frame must already have reached the consumer.
	f.inject(streamFixturePacket(audio, bob, 50))
	second := nextStreamEvent(t, f.stream)
	if second.Metadata.OriginGUID != bob || second.TransmissionID == first.TransmissionID {
		t.Fatal("overlapping speakers must retain independent transmissions")
	}
	f.inject(streamFixturePacket(audio, alice, 12))
	third := nextStreamEvent(t, f.stream)
	if third.TransmissionID != first.TransmissionID || third.LostPackets != 1 {
		t.Fatalf("missing packet is explicit, not buffered indefinitely: %+v", third)
	}
	// Late/reordered and duplicate relay packets must not rewind either decoder.
	f.inject(streamFixturePacket(audio, alice, 11))
	f.inject(p)
	f.inject(streamFixturePacket(audio, bob, 51))
	fourth := nextStreamEvent(t, f.stream)
	if fourth.Metadata.OriginGUID != bob || fourth.Metadata.PacketID != 51 {
		t.Fatalf("late/duplicate frame leaked: %+v", fourth)
	}
	// The external-AWACS sentinel is preserved but must not become an aircraft binding.
	p = streamFixturePacket(audio, bob, 52)
	p.UnitID = 100000002
	f.inject(p)
	if event := nextStreamEvent(t, f.stream); event.Metadata.AircraftUnitIDKnown {
		t.Fatal("external AWACS ID falsely claims an aircraft")
	}
}

func TestStreamSuppressesSelfAndDuplicateRelays(t *testing.T) {
	f := newStreamingFixture(t, nil)
	audio := streamFixtureAudio(t)
	other := types.GUID("1111111111111111111111")
	f.inject(streamFixturePacket(audio, f.self, 1))
	p := streamFixturePacket(audio, other, 1)
	f.inject(p)
	if e := nextStreamEvent(t, f.stream); e.Metadata.OriginGUID != other {
		t.Fatal("self audio was received")
	}
	p.RelayGUID = []byte("2222222222222222222222")
	p.Hops = 1
	f.inject(p)
	f.inject(streamFixturePacket(audio, other, 2))
	if e := nextStreamEvent(t, f.stream); e.Metadata.PacketID != 2 {
		t.Fatal("duplicate relay was received")
	}
	assertNoStreamEvent(t, f.stream)
}

func TestStreamTransmitsBeforeProducerCompletesAndPaces(t *testing.T) {
	f := newStreamingFixture(t, nil)
	frames := make(chan StreamFrame, 2)
	frames <- StreamFrame{PCM: testTone(640)}
	frames <- StreamFrame{PCM: testTone(640)}
	result := make(chan error, 1)
	go func() { result <- f.stream.Transmit(context.Background(), frames) }()
	first := nextStreamVoice(t, f.udp)
	if first.PacketID == 0 || len(first.AudioBytes) == 0 || types.GUID(first.OriginGUID) != f.self {
		t.Fatalf("invalid first frame: %+v", first)
	}
	// The producer channel remains open while the first packet is already sent.
	select {
	case err := <-result:
		t.Fatalf("producer unexpectedly complete: %v", err)
	default:
	}
	now := f.clock.Now()
	f.clock.awaitDeadlineCount(t, now.Add(40*time.Millisecond), 2)
	f.clock.advance(39 * time.Millisecond)
	select {
	case b := <-f.udp.outgoing:
		if len(b) > types.GUIDLength {
			t.Fatal("second packet transmitted before its deadline")
		}
	default:
	}
	f.clock.advance(time.Millisecond)
	second := nextStreamVoice(t, f.udp)
	if second.PacketID != first.PacketID+1 {
		t.Fatal("packet sequence not monotonic")
	}
	close(frames)
	// Close terminates the source rather than synthesizing silence forever.
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("producer close did not finish transmission")
	}
}

func TestStreamCancellationDisconnectAndFreshSession(t *testing.T) {
	f := newStreamingFixture(t, nil)
	frames := make(chan StreamFrame)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- f.stream.Transmit(ctx, frames) }()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("transmission cancellation blocked")
	}
	_ = f.server.Close()
	select {
	case <-f.stream.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("TCP disconnect did not stop session")
	}
	if f.stream.Err() == nil {
		t.Fatal("disconnect must be distinguishable from gap completion")
	}
	if err := f.stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.stream.Close(); err != nil {
		t.Fatal(err)
	}
	fresh := newStreamingFixture(t, nil)
	if fresh.self == f.self {
		t.Fatal("fresh connection reused previous session identity")
	}
	audio := streamFixtureAudio(t)
	fresh.inject(streamFixturePacket(audio, types.GUID("1111111111111111111111"), 1))
	if e := nextStreamEvent(t, fresh.stream); e.Metadata.PacketID != 1 {
		t.Fatal("new session retained stale packet high-watermark")
	}
}

func TestStreamRejectsInvalidFramesWithoutSending(t *testing.T) {
	for _, frame := range []StreamFrame{{PCM: make([]float32, 639)}, {PCM: make([]float32, 640), Opus: []byte{1}}, {}, {Opus: []byte{0xde, 0xad, 0xbe, 0xef}}} {
		t.Run("invalid", func(t *testing.T) {
			f := newStreamingFixture(t, nil)
			frames := make(chan StreamFrame, 1)
			frames <- frame
			close(frames)
			if err := f.stream.Transmit(context.Background(), frames); err == nil {
				t.Fatal("invalid frame accepted")
			}
			select {
			case b := <-f.udp.outgoing:
				if len(b) > types.GUIDLength {
					t.Fatal("invalid frame sent")
				}
			default:
			}
		})
	}
}

func TestStreamDialFailureDoesNotExposeCredential(t *testing.T) {
	cfg := types.ClientConfiguration{Address: "fixture.invalid:5002", ClientName: "fixture", Coalition: coalitions.Blue, ExternalAWACSModePassword: "private-value", Radios: []types.Radio{{Frequency: 251000000, Modulation: types.ModulationAM}}}
	_, err := ConnectStream(context.Background(), cfg, StreamOptions{Dial: func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("private-value") }})
	if err == nil || strings.Contains(err.Error(), "private-value") {
		t.Fatalf("unsafe network error: %v", err)
	}
}

var _ io.Closer = (*streamDatagram)(nil)

func TestStreamGapCompletionRetainsDuplicateHighWatermark(t *testing.T) {
	f := newStreamingFixture(t, nil)
	audio := streamFixtureAudio(t)
	sender := types.GUID("1111111111111111111111")
	f.inject(streamFixturePacket(audio, sender, 10))
	first := nextStreamEvent(t, f.stream)
	f.clock.awaitDeadline(t, f.clock.Now().Add(StreamFrameDuration))
	f.clock.advance(300 * time.Millisecond)
	gap := nextStreamEvent(t, f.stream)
	if gap.Kind != StreamGap || gap.TransmissionID != first.TransmissionID || gap.Metadata.PacketID != 10 {
		t.Fatalf("bad completion: %+v", gap)
	}
	select {
	case <-f.stream.Done():
		t.Fatal("gap completion disconnected transport")
	default:
	}
	f.inject(streamFixturePacket(audio, sender, 10))
	f.inject(streamFixturePacket(audio, sender, 11))
	second := nextStreamEvent(t, f.stream)
	if second.Kind != StreamAudio || second.Metadata.PacketID != 11 || second.TransmissionID == first.TransmissionID {
		t.Fatalf("late duplicate restarted stream: %+v", second)
	}
}

func TestStreamUnderrunAndBusyProducer(t *testing.T) {
	f := newStreamingFixture(t, nil)
	frames := make(chan StreamFrame, 1)
	result := make(chan error, 1)
	go func() { result <- f.stream.Transmit(context.Background(), frames) }()
	f.clock.awaitDeadline(t, f.clock.Now().Add(200*time.Millisecond))
	if err := f.stream.Transmit(context.Background(), make(chan StreamFrame)); !errors.Is(err, ErrStreamBusy) {
		t.Fatalf("concurrent producer: %v", err)
	}
	f.clock.advance(200 * time.Millisecond)
	select {
	case err := <-result:
		if !errors.Is(err, ErrStreamUnderrun) {
			t.Fatalf("underrun: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("underrun not detected")
	}
	frames <- StreamFrame{PCM: testTone(640)}
	select {
	case b := <-f.udp.outgoing:
		if len(b) > types.GUIDLength {
			t.Fatal("late producer audio replayed after underrun")
		}
	default:
	}
}

func TestStreamReceiveQueueOverflowDisconnectsWithoutStaleReplay(t *testing.T) {
	f := newStreamingFixture(t, func(o *StreamOptions) { o.ReceiveQueue = 1 })
	audio := streamFixtureAudio(t)
	sender := types.GUID("1111111111111111111111")
	f.inject(streamFixturePacket(audio, sender, 1))
	f.inject(streamFixturePacket(audio, sender, 2))
	select {
	case <-f.stream.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("overflow did not terminate bounded queue")
	}
	if !errors.Is(f.stream.Err(), ErrStreamQueueFull) {
		t.Fatalf("queue overflow: %v", f.stream.Err())
	}
	if _, ok := <-f.stream.Receive(); ok {
		t.Fatal("dead epoch retained stale audio")
	}
}

func TestStreamSenderLimitFailsVisibly(t *testing.T) {
	f := newStreamingFixture(t, func(o *StreamOptions) { o.MaxSenders = 1 })
	audio := streamFixtureAudio(t)
	f.inject(streamFixturePacket(audio, types.GUID("1111111111111111111111"), 1))
	_ = nextStreamEvent(t, f.stream)
	f.inject(streamFixturePacket(audio, types.GUID("2222222222222222222222"), 1))
	select {
	case <-f.stream.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("sender cap did not fail visibly")
	}
	if !errors.Is(f.stream.Err(), ErrStreamQueueFull) {
		t.Fatalf("sender cap: %v", f.stream.Err())
	}
}

func TestStreamRoutingAndEncryptionFiltering(t *testing.T) {
	f := newStreamingFixture(t, nil)
	audio := streamFixtureAudio(t)
	sender := types.GUID("1111111111111111111111")
	wrongFreq := streamFixturePacket(audio, sender, 1)
	wrongFreq.Frequencies[0].Frequency = 252000000
	f.inject(wrongFreq)
	wrongMod := streamFixturePacket(audio, sender, 2)
	wrongMod.Frequencies[0].Modulation = byte(types.ModulationFM)
	f.inject(wrongMod)
	encrypted := streamFixturePacket(audio, sender, 3)
	encrypted.Frequencies[0].Encryption = 7
	f.inject(encrypted)
	f.inject(streamFixturePacket(audio, sender, 4))
	e := nextStreamEvent(t, f.stream)
	if e.Metadata.PacketID != 4 {
		t.Fatalf("off-channel or undecryptable audio accepted: %+v", e)
	}
}

// The second net.Pipe write is a processing barrier: Scanner cannot read it
// until it has applied the first complete message from the preceding write.
func (f *streamingFixture) serverMessage(t *testing.T, m types.Message) {
	t.Helper()
	if err := json.NewEncoder(f.server).Encode(m); err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(f.server).Encode(types.Message{Type: types.MessagePing}); err != nil {
		t.Fatal(err)
	}
}
func TestStreamCoalitionMetadataAndDisconnectFiltering(t *testing.T) {
	f := newStreamingFixture(t, nil)
	sender := types.GUID("1111111111111111111111")
	other := types.GUID("2222222222222222222222")
	audio := streamFixtureAudio(t)
	f.serverMessage(t, types.Message{Type: types.MessageServerSettings, ServerSettings: map[string]string{string(types.CoalitionAudioSecurity): "true"}})
	f.serverMessage(t, types.Message{Type: types.MessageUpdate, Client: &types.ClientInfo{GUID: other, Coalition: coalitions.Blue}})
	f.inject(streamFixturePacket(audio, sender, 1)) // Unknown sender cannot pass secure-coalition filtering.
	// UDP injection only queues a packet; it does not wait for dispatch. Observe
	// the following known sender as a FIFO barrier before making sender known,
	// so the metadata update cannot race ahead of the unknown packet's filter.
	f.inject(streamFixturePacket(audio, other, 1))
	event := nextStreamEvent(t, f.stream)
	if event.Metadata.OriginGUID != other || event.Metadata.PacketID != 1 {
		t.Fatalf("unknown sender passed secure-coalition filtering: %+v", event)
	}
	f.serverMessage(t, types.Message{Type: types.MessageUpdate, Client: &types.ClientInfo{GUID: sender, Coalition: coalitions.Blue}})
	f.inject(streamFixturePacket(audio, sender, 2))
	event = nextStreamEvent(t, f.stream)
	if event.Metadata.OriginGUID != sender || event.Metadata.PacketID != 2 || !event.Metadata.CoalitionKnown || event.Metadata.Coalition != coalitions.Blue {
		t.Fatalf("coalition metadata/filter: %+v", event)
	}
	f.serverMessage(t, types.Message{Type: types.MessageClientDisconnect, Client: &types.ClientInfo{GUID: sender}})
	f.inject(streamFixturePacket(audio, sender, 3))
	// The same known sender proves dispatch passed the disconnected peer's
	// packet without changing either peer's metadata during that dispatch.
	f.inject(streamFixturePacket(audio, other, 2))
	event = nextStreamEvent(t, f.stream)
	if event.Metadata.OriginGUID != other || event.Metadata.PacketID != 2 {
		t.Fatalf("disconnected peer retained known coalition: %+v", event)
	}
}

func TestStreamCancelDuringPacingDoesNotSendLateFrame(t *testing.T) {
	f := newStreamingFixture(t, nil)
	frames := make(chan StreamFrame, 2)
	frames <- StreamFrame{PCM: testTone(640)}
	frames <- StreamFrame{PCM: testTone(640)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- f.stream.Transmit(ctx, frames) }()
	_ = nextStreamVoice(t, f.udp)
	f.clock.awaitDeadlineCount(t, f.clock.Now().Add(StreamFrameDuration), 2)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel during pace: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pacing blocked cancellation")
	}
	f.clock.advance(40 * time.Millisecond)
	select {
	case b := <-f.udp.outgoing:
		if len(b) > types.GUIDLength {
			t.Fatal("cancelled frame was transmitted later")
		}
	default:
	}
}

func TestStreamPeerQueueLimitDisconnects(t *testing.T) {
	f := newStreamingFixture(t, func(o *StreamOptions) { o.MaxPeers = 1 })
	message := types.Message{Type: types.MessageSync, Clients: []types.ClientInfo{{GUID: "1111111111111111111111", Coalition: coalitions.Blue}, {GUID: "2222222222222222222222", Coalition: coalitions.Blue}}}
	if err := json.NewEncoder(f.server).Encode(message); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.stream.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("peer state cap not enforced")
	}
	if !errors.Is(f.stream.Err(), ErrStreamQueueFull) {
		t.Fatalf("peer cap: %v", f.stream.Err())
	}
}

func TestStreamInvalidConfigurationNeverDials(t *testing.T) {
	calls := 0
	dial := func(context.Context, string, string) (net.Conn, error) { calls++; return nil, errors.New("unexpected") }
	cfg := types.ClientConfiguration{Address: "fixture.invalid:5002", ClientName: "fixture", Coalition: coalitions.Blue, Radios: []types.Radio{{Frequency: 251000000, Modulation: types.ModulationAM}}}
	for _, opts := range []StreamOptions{{ReceiveQueue: 4097}, {PacketQueue: -1}, {MaxSenders: 4097}, {MaxPeers: 65537}, {Gap: time.Millisecond}, {Underrun: 11 * time.Second}} {
		opts.Dial = dial
		if _, err := ConnectStream(context.Background(), cfg, opts); !errors.Is(err, ErrStreamConfiguration) {
			t.Fatalf("invalid options: %v", err)
		}
	}
	if calls != 0 {
		t.Fatal("invalid options opened network")
	}
}

func TestStreamUDPConnectionFailureClosesTCP(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	cfg := types.ClientConfiguration{Address: "fixture.invalid:5002", ClientName: "fixture", Coalition: coalitions.Blue, Radios: []types.Radio{{Frequency: 251000000, Modulation: types.ModulationAM}}}
	_, err := ConnectStream(context.Background(), cfg, StreamOptions{Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		if network == "tcp" {
			return client, nil
		}
		return nil, errors.New("fixture UDP unavailable")
	}})
	if !errors.Is(err, ErrStreamDisconnected) {
		t.Fatalf("dial: %v", err)
	}
	if _, err = server.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("TCP leaked after UDP dial failure: %v", err)
	}
}

func TestStreamCompletionMetadataOwnership(t *testing.T) {
	f := newStreamingFixture(t, nil)
	audio := streamFixtureAudio(t)
	sender := types.GUID("1111111111111111111111")
	f.inject(streamFixturePacket(audio, sender, 1))
	event := nextStreamEvent(t, f.stream)
	event.Metadata.Frequencies[0].Frequency = 123
	event.Frame.Opus[0] ^= 0xff
	event.Frame.PCM[0] = 1
	f.clock.awaitDeadline(t, f.clock.Now().Add(StreamFrameDuration))
	f.clock.advance(300 * time.Millisecond)
	gap := nextStreamEvent(t, f.stream)
	if gap.Kind != StreamGap || gap.Metadata.Frequencies[0].Frequency != 251000000 {
		t.Fatal("consumer mutated transport-owned completion metadata")
	}
}

func TestStreamRejectsMalformedRoutesWithoutOpeningTransmission(t *testing.T) {
	f := newStreamingFixture(t, nil)
	audio := streamFixtureAudio(t)
	sender := types.GUID("1111111111111111111111")
	for i, freq := range []float64{math.NaN(), 251000000.5, math.Inf(1), -1} {
		p := voice.NewPacket(audio, []voice.Frequency{{Frequency: 251000000, Modulation: 0}, {Frequency: freq, Modulation: 0}}, 42, uint64(i+1), 0, []byte(sender), []byte(sender))
		f.inject(p)
	}
	f.inject(streamFixturePacket([]byte{0xde, 0xad, 0xbe, 0xef}, sender, 5))
	f.inject(streamFixturePacket(audio, sender, 6))
	event := nextStreamEvent(t, f.stream)
	if event.Kind != StreamAudio || event.Metadata.PacketID != 6 || event.TransmissionID != 1 {
		t.Fatalf("malformed packet opened a transmission: %+v", event)
	}
}

func TestStreamContextCancellationPrecedesClosedProducer(t *testing.T) {
	f := newStreamingFixture(t, nil)
	frames := make(chan StreamFrame)
	close(frames)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.stream.Transmit(ctx, frames); !errors.Is(err, context.Canceled) {
		t.Fatalf("closed producer hid cancellation: %v", err)
	}
	_ = f.stream.Close()
	if err := f.stream.Transmit(context.Background(), frames); err == nil {
		t.Fatal("closed producer hid disconnected session")
	}
}

type observedVoiceConn struct {
	net.Conn
	entered chan struct{}
}

func (c *observedVoiceConn) Write(b []byte) (int, error) {
	if len(b) > types.GUIDLength {
		select {
		case c.entered <- struct{}{}:
		default:
		}
	}
	return c.Conn.Write(b)
}
func TestStreamCancellationInterruptsBlockedNetworkWrite(t *testing.T) {
	clientUDP, serverUDP := net.Pipe()
	defer serverUDP.Close()
	observed := &observedVoiceConn{Conn: clientUDP, entered: make(chan struct{}, 1)}
	// Consume the initial UDP ping, then deliberately stop reading the wire.
	go func() { _, _ = io.ReadFull(serverUDP, make([]byte, types.GUIDLength)) }()
	f := newStreamingFixture(t, func(o *StreamOptions) {
		original := o.Dial
		o.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			if network == "udp" {
				return observed, nil
			}
			return original(ctx, network, address)
		}
	})
	frames := make(chan StreamFrame, 1)
	frames <- StreamFrame{PCM: testTone(640)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- f.stream.Transmit(ctx, frames) }()
	select {
	case <-observed.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("voice write did not reach blocked wire")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked write cancel: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("network write ignored transmission cancellation")
	}
}

func TestStreamAuthenticationRejectsWrongCoalitionAndRedactsSecrets(t *testing.T) {
	tcp, server := net.Pipe()
	defer server.Close()
	udp := newStreamDatagram()
	go func() {
		scanner := bufio.NewScanner(server)
		for scanner.Scan() {
			var m types.Message
			_ = json.Unmarshal(scanner.Bytes(), &m)
			if m.Type == types.MessageExternalAWACSModePassword {
				_ = json.NewEncoder(server).Encode(types.Message{Type: types.MessageExternalAWACSModePassword, ExternalAWACSModePassword: "private-value", Client: &types.ClientInfo{Coalition: coalitions.Red}})
			}
		}
	}()
	cfg := types.ClientConfiguration{Address: "fixture.invalid:5002", ClientName: "fixture", Coalition: coalitions.Blue, ExternalAWACSModePassword: "private-value", Radios: []types.Radio{{Frequency: 251000000, Modulation: types.ModulationAM}}}
	s, err := ConnectStream(context.Background(), cfg, StreamOptions{Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		if network == "tcp" {
			return tcp, nil
		}
		return udp, nil
	}})
	if s != nil || !errors.Is(err, ErrStreamAuthentication) || strings.Contains(err.Error(), "private-value") {
		t.Fatalf("unsafe auth result: session=%v error=%v", s, err)
	}
	select {
	case <-udp.closed:
	default:
		t.Fatal("failed authentication leaked UDP connection")
	}
}

func TestStreamErasesCredentialAfterHandshake(t *testing.T) {
	f := newStreamingFixture(t, nil)
	if f.stream.config.ExternalAWACSModePassword != "" {
		t.Fatal("runtime retained handshake credential")
	}
	// Credential injection occurs only in the authentication message.
	select {
	case m := <-f.messages:
		if m.Type != types.MessageExternalAWACSModePassword || m.ExternalAWACSModePassword != "fixture-password" {
			t.Fatal("credential was not injected at authentication boundary")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing authentication message")
	}
}

func TestStreamPacingSpansProducerCallsWithoutCatchupBurst(t *testing.T) {
	f := newStreamingFixture(t, nil)
	one := make(chan StreamFrame, 1)
	one <- StreamFrame{PCM: testTone(640)}
	close(one)
	if err := f.stream.Transmit(context.Background(), one); err != nil {
		t.Fatal(err)
	}
	first := nextStreamVoice(t, f.udp)
	two := make(chan StreamFrame, 2)
	two <- StreamFrame{PCM: testTone(640)}
	two <- StreamFrame{PCM: testTone(640)}
	close(two)
	result := make(chan error, 1)
	go func() { result <- f.stream.Transmit(context.Background(), two) }()
	f.clock.awaitDeadlineCount(t, f.clock.Now().Add(StreamFrameDuration), 2)
	f.clock.advance(time.Second)
	second := nextStreamVoice(t, f.udp)
	if second.PacketID != first.PacketID+1 {
		t.Fatal("producer boundary reset packet sequence")
	}
	// A large clock jump must not let both overdue packets leave in a burst.
	f.clock.awaitDeadlineCount(t, f.clock.Now().Add(StreamFrameDuration), 2)
	select {
	case b := <-f.udp.outgoing:
		if len(b) > types.GUIDLength {
			t.Fatal("clock jump caused catchup burst")
		}
	default:
	}
	f.clock.advance(StreamFrameDuration)
	third := nextStreamVoice(t, f.udp)
	if third.PacketID != second.PacketID+1 {
		t.Fatal("packet order changed after delayed pacing")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("producer call did not finish")
	}
}

func TestStreamPacketQueueOverflowIsBoundedAndVisible(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcp, server := net.Pipe()
	defer server.Close()
	udp := newStreamDatagram()
	s := &Stream{tcp: tcp, udp: udp, ctx: ctx, cancel: cancel, packets: make(chan []byte, 1)}
	s.packets <- []byte("already queued")
	stopped := make(chan struct{})
	go func() { s.readUDP(); close(stopped) }()
	udp.incoming <- make([]byte, types.GUIDLength+1)
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("full packet queue blocked UDP reader")
	}
	if !errors.Is(s.Err(), ErrStreamQueueFull) || len(s.packets) != 1 {
		t.Fatalf("packet queue overflow: len=%d err=%v", len(s.packets), s.Err())
	}
}

func TestStreamAuthenticationTimeoutClosesConnections(t *testing.T) {
	tcp, server := net.Pipe()
	defer server.Close()
	udp := newStreamDatagram()
	clock := newManualStreamClock()
	go func() {
		scanner := bufio.NewScanner(server)
		for scanner.Scan() { /* Deliberately never authenticate. */
		}
	}()
	cfg := types.ClientConfiguration{Address: "fixture.invalid:5002", ClientName: "fixture", Coalition: coalitions.Blue, ExternalAWACSModePassword: "private-value", ConnectionTimeout: 500 * time.Millisecond, Radios: []types.Radio{{Frequency: 251000000, Modulation: types.ModulationAM}}}
	result := make(chan error, 1)
	go func() {
		_, err := ConnectStream(context.Background(), cfg, StreamOptions{Clock: clock, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			if network == "tcp" {
				return tcp, nil
			}
			return udp, nil
		}})
		result <- err
	}()
	clock.awaitDeadline(t, clock.Now().Add(500*time.Millisecond))
	clock.advance(500 * time.Millisecond)
	select {
	case err := <-result:
		if !errors.Is(err, ErrStreamAuthentication) || strings.Contains(err.Error(), "private-value") {
			t.Fatalf("authentication timeout: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("authentication timeout leaked connection")
	}
	select {
	case <-udp.closed:
	default:
		t.Fatal("authentication timeout did not close UDP")
	}
}
