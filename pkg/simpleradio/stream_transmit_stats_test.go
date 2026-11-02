package simpleradio

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dharmab/skyeye/pkg/simpleradio/types"
)

type streamStatsResult struct {
	stats StreamTransmitStats
	err   error
}

func awaitStreamStats(t *testing.T, done <-chan streamStatsResult) streamStatsResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("transmit with stats did not finish")
		return streamStatsResult{}
	}
}

func TestStreamTransmitStatsBoundedIntervals(t *testing.T) {
	f := newStreamingFixture(t, nil)
	frames := make(chan StreamFrame, 38)
	for range cap(frames) {
		frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	}
	close(frames)
	done := make(chan streamStatsResult, 1)
	go func() {
		stats, err := f.stream.TransmitWithStats(context.Background(), frames)
		done <- streamStatsResult{stats, err}
	}()
	firstAt := f.clock.Now()
	nextStreamVoice(t, f.udp)
	intervals := []time.Duration{60 * time.Millisecond, 100 * time.Millisecond, 101 * time.Millisecond}
	for range 34 {
		intervals = append(intervals, 61*time.Millisecond)
	}
	var elapsed time.Duration
	for _, gap := range intervals {
		// The dispatcher also has a 40 ms timer. Await both so advancing the
		// manual clock cannot precede registration of the next pacing timer.
		f.clock.awaitDeadlineCount(t, f.clock.Now().Add(StreamFrameDuration), 2)
		f.clock.advance(gap)
		elapsed += gap
		nextStreamVoice(t, f.udp)
	}
	result := awaitStreamStats(t, done)
	s := result.stats
	if result.err != nil {
		t.Fatal(result.err)
	}
	if s.Packets != 38 || !s.FirstWriteAt.Equal(firstAt) || !s.LastWriteAt.Equal(firstAt.Add(elapsed)) || s.SumGap != elapsed || s.MaxGap != 101*time.Millisecond {
		t.Fatalf("incorrect successful-write observations: %+v", s)
	}
	if s.GapsOver60MS != 36 || s.GapsOver100MS != 1 || len(s.GapEvents) != 32 || s.GapEventsDropped != 4 {
		t.Fatalf("thresholds or bounds incorrect: %+v", s)
	}
	if s.GapEvents[0] != (StreamTransmitGap{AfterPacket: 2, Elapsed: 160 * time.Millisecond, Gap: 100 * time.Millisecond}) || s.GapEvents[1] != (StreamTransmitGap{AfterPacket: 3, Elapsed: 261 * time.Millisecond, Gap: 101 * time.Millisecond}) {
		t.Fatalf("gap event positions incorrect: %+v", s.GapEvents[:2])
	}
}

func TestStreamTransmitStatsExposeAccumulatedPacingDelay(t *testing.T) {
	f := newStreamingFixture(t, nil)
	frames := make(chan StreamFrame, 100)
	for range cap(frames) {
		frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	}
	close(frames)
	done := make(chan streamStatsResult, 1)
	go func() {
		stats, err := f.stream.TransmitWithStats(context.Background(), frames)
		done <- streamStatsResult{stats, err}
	}()
	nextStreamVoice(t, f.udp)
	for range 99 {
		f.clock.awaitDeadlineCount(t, f.clock.Now().Add(StreamFrameDuration), 2)
		// Model a timer waking one ms late, with every producer frame ready.
		// Current minimum-spacing pacing starts the next period at that wake;
		// stats must expose its accumulated delay even without a >60 ms gap.
		f.clock.advance(StreamFrameDuration + time.Millisecond)
		nextStreamVoice(t, f.udp)
	}
	result := awaitStreamStats(t, done)
	s := result.stats
	if result.err != nil || s.Packets != 100 || s.SumGap != 99*41*time.Millisecond || s.LastWriteAt.Sub(s.FirstWriteAt) != s.SumGap || s.MaxGap != 41*time.Millisecond || s.GapsOver60MS != 0 || len(s.GapEvents) != 0 {
		t.Fatalf("pacing delay observations: stats=%+v err=%v", s, result.err)
	}
	t.Logf("100 ready packets with 1 ms late pacing wakes: observed=%v nominal=%v delay=%v", s.SumGap, 99*StreamFrameDuration, s.SumGap-99*StreamFrameDuration)
}

func TestStreamTransmitStatsPartialAndEmptyResults(t *testing.T) {
	for _, test := range []struct {
		name    string
		first   bool
		invalid bool
		cancel  bool
		wantErr error
	}{
		{name: "empty EOF"},
		{name: "invalid first", invalid: true, wantErr: ErrStreamFrame},
		{name: "canceled before first", cancel: true, wantErr: context.Canceled},
		{name: "successful EOF", first: true},
		{name: "invalid after first", first: true, invalid: true, wantErr: ErrStreamFrame},
		{name: "canceled after first", first: true, cancel: true, wantErr: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newStreamingFixture(t, nil)
			frames := make(chan StreamFrame, 2)
			if test.first {
				frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
			}
			if test.invalid {
				frames <- StreamFrame{}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if !test.cancel {
				close(frames)
			} else if !test.first {
				cancel()
			}
			done := make(chan streamStatsResult, 1)
			go func() {
				stats, err := f.stream.TransmitWithStats(ctx, frames)
				done <- streamStatsResult{stats, err}
			}()
			if test.first && test.cancel {
				nextStreamVoice(t, f.udp)
				f.clock.awaitDeadline(t, f.clock.Now().Add(f.stream.opts.Underrun))
				cancel()
			}
			result := awaitStreamStats(t, done)
			if !errors.Is(result.err, test.wantErr) {
				t.Fatalf("error %v, want %v", result.err, test.wantErr)
			}
			if test.first {
				if result.stats.Packets != 1 || !result.stats.FirstWriteAt.Equal(f.clock.Now()) || result.stats.LastWriteAt != result.stats.FirstWriteAt || result.stats.SumGap != 0 || len(result.stats.GapEvents) != 0 {
					t.Fatalf("lost or invented partial observations: %+v", result.stats)
				}
			} else if result.stats.Packets != 0 || !result.stats.FirstWriteAt.IsZero() || !result.stats.LastWriteAt.IsZero() || result.stats.SumGap != 0 || len(result.stats.GapEvents) != 0 {
				t.Fatalf("unwritten audio was counted: %+v", result.stats)
			}
		})
	}
}

type statsWriteConn struct {
	net.Conn
	clock  *manualStreamClock
	writes int
}

func (c *statsWriteConn) Write(p []byte) (int, error) {
	if len(p) <= types.GUIDLength {
		return c.Conn.Write(p)
	}
	c.writes++
	if c.writes == 2 {
		return 0, net.ErrClosed
	}
	n, err := c.Conn.Write(p)
	if err == nil {
		// The write has returned from the underlying datagram, but its caller
		// cannot observe success until this wrapper returns seven ms later.
		c.clock.advance(7 * time.Millisecond)
	}
	return n, err
}

func TestStreamTransmitStatsUseSuccessfulWriteCompletion(t *testing.T) {
	f := newStreamingFixture(t, func(o *StreamOptions) {
		original := o.Dial
		o.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := original(ctx, network, address)
			if network == "udp" && err == nil {
				return &statsWriteConn{Conn: conn, clock: o.Clock.(*manualStreamClock)}, nil
			}
			return conn, err
		}
	})
	startedAt := f.clock.Now()
	frames := make(chan StreamFrame, 2)
	for range cap(frames) {
		frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	}
	close(frames)
	done := make(chan streamStatsResult, 1)
	go func() {
		stats, err := f.stream.TransmitWithStats(context.Background(), frames)
		done <- streamStatsResult{stats, err}
	}()
	nextStreamVoice(t, f.udp)
	writtenAt := startedAt.Add(7 * time.Millisecond)
	f.clock.awaitDeadline(t, writtenAt.Add(StreamFrameDuration))
	f.clock.advance(StreamFrameDuration)
	result := awaitStreamStats(t, done)
	if !errors.Is(result.err, ErrStreamDisconnected) || result.stats.Packets != 1 || !result.stats.FirstWriteAt.Equal(writtenAt) || result.stats.LastWriteAt != result.stats.FirstWriteAt {
		t.Fatalf("write failure or completion timestamp incorrect: stats=%+v err=%v", result.stats, result.err)
	}
}

func TestStreamTransmitStatsDoNotCountBlockedCanceledWrite(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	observed := &observedVoiceConn{Conn: client, entered: make(chan struct{}, 1)}
	go func() { _, _ = io.ReadFull(server, make([]byte, types.GUIDLength)) }()
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
	frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan streamStatsResult, 1)
	go func() {
		stats, err := f.stream.TransmitWithStats(ctx, frames)
		done <- streamStatsResult{stats, err}
	}()
	select {
	case <-observed.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("write did not reach blocked connection")
	}
	cancel()
	result := awaitStreamStats(t, done)
	if !errors.Is(result.err, context.Canceled) || result.stats.Packets != 0 || !result.stats.FirstWriteAt.IsZero() || !result.stats.LastWriteAt.IsZero() {
		t.Fatalf("blocked write counted as successful: stats=%+v err=%v", result.stats, result.err)
	}
}

func TestStreamTransmitStatsShareProducerGuardAndResetBetweenCalls(t *testing.T) {
	f := newStreamingFixture(t, nil)
	frames := make(chan StreamFrame, 1)
	frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan streamStatsResult, 1)
	go func() {
		stats, err := f.stream.TransmitWithStats(ctx, frames)
		done <- streamStatsResult{stats, err}
	}()
	nextStreamVoice(t, f.udp)
	f.clock.awaitDeadline(t, f.clock.Now().Add(f.stream.opts.Underrun))
	if err := f.stream.Transmit(context.Background(), frames); !errors.Is(err, ErrStreamBusy) {
		t.Fatalf("legacy producer bypassed guard: %v", err)
	}
	stats, err := f.stream.TransmitWithStats(context.Background(), frames)
	if !errors.Is(err, ErrStreamBusy) || stats.Packets != 0 {
		t.Fatalf("observed producer bypassed guard: stats=%+v err=%v", stats, err)
	}
	cancel()
	if result := awaitStreamStats(t, done); !errors.Is(result.err, context.Canceled) || result.stats.Packets != 1 {
		t.Fatalf("active producer observations: %+v", result)
	}
	f.clock.advance(time.Second)
	frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	close(frames)
	stats, err = f.stream.TransmitWithStats(context.Background(), frames)
	if err != nil || stats.Packets != 1 || !stats.FirstWriteAt.Equal(f.clock.Now()) || stats.LastWriteAt != stats.FirstWriteAt || stats.SumGap != 0 || len(stats.GapEvents) != 0 {
		t.Fatalf("per-call stats retained prior transmission: stats=%+v err=%v", stats, err)
	}
}
