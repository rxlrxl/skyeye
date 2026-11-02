package simpleradio

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/dharmab/skyeye/pkg/simpleradio/types"
)

func assertNoStreamVoiceWrite(t *testing.T, udp *streamDatagram) {
	t.Helper()
	for {
		select {
		case p := <-udp.outgoing:
			if len(p) > types.GUIDLength {
				t.Fatal("voice packet written before pacing deadline")
			}
		default:
			return
		}
	}
}

func startPacedFrames(stream *Stream, ctx context.Context, frames <-chan StreamFrame) <-chan streamStatsResult {
	done := make(chan streamStatsResult, 1)
	go func() {
		stats, err := stream.TransmitWithStats(ctx, frames)
		done <- streamStatsResult{stats, err}
	}()
	return done
}

func TestStreamAbsolutePacingBoundsCorrectionAndResetsBeyondLimit(t *testing.T) {
	for _, late := range []time.Duration{streamPacingCatchupLimit, streamPacingCatchupLimit + time.Nanosecond} {
		t.Run(late.String(), func(t *testing.T) {
			f := newStreamingFixture(t, nil)
			frames := make(chan StreamFrame, 3)
			for range cap(frames) {
				frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
			}
			close(frames)
			done := startPacedFrames(f.stream, context.Background(), frames)
			nextStreamVoice(t, f.udp)
			f.clock.awaitDeadlineCount(t, f.clock.Now().Add(StreamFrameDuration), 2)
			f.clock.advance(StreamFrameDuration + late)
			nextStreamVoice(t, f.udp)
			nextDelay := StreamFrameDuration - late
			count := 1
			if late > streamPacingCatchupLimit {
				nextDelay = StreamFrameDuration
				count = 2
			}
			f.clock.awaitDeadlineCount(t, f.clock.Now().Add(nextDelay), count)
			f.clock.advance(nextDelay - time.Millisecond)
			assertNoStreamVoiceWrite(t, f.udp)
			f.clock.advance(time.Millisecond)
			nextStreamVoice(t, f.udp)
			result := awaitStreamStats(t, done)
			wantSum := StreamFrameDuration + late + nextDelay
			if result.err != nil || result.stats.Packets != 3 || result.stats.SumGap != wantSum || result.stats.MaxGap != StreamFrameDuration+late {
				t.Fatalf("bounded pacing observations: stats=%+v err=%v", result.stats, result.err)
			}
		})
	}
}

func TestStreamAbsolutePacingReanchorsAfterProducerStall(t *testing.T) {
	f := newStreamingFixture(t, nil)
	frames := make(chan StreamFrame, 2)
	frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	done := startPacedFrames(f.stream, context.Background(), frames)
	nextStreamVoice(t, f.udp)
	f.clock.awaitDeadline(t, f.clock.Now().Add(f.stream.opts.Underrun))
	f.clock.advance(120 * time.Millisecond)
	frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	close(frames)
	nextStreamVoice(t, f.udp)
	f.clock.awaitDeadlineCount(t, f.clock.Now().Add(StreamFrameDuration), 2)
	f.clock.advance(39 * time.Millisecond)
	assertNoStreamVoiceWrite(t, f.udp)
	f.clock.advance(time.Millisecond)
	nextStreamVoice(t, f.udp)
	result := awaitStreamStats(t, done)
	if result.err != nil || result.stats.Packets != 3 || result.stats.SumGap != 160*time.Millisecond || result.stats.MaxGap != 120*time.Millisecond {
		t.Fatalf("producer stall caused catchup: stats=%+v err=%v", result.stats, result.err)
	}
}

func TestStreamAbsolutePacingPreservesActualWriteFloorAcrossCalls(t *testing.T) {
	f := newStreamingFixture(t, nil)
	first := make(chan StreamFrame, 2)
	for range cap(first) {
		first <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	}
	close(first)
	done := startPacedFrames(f.stream, context.Background(), first)
	nextStreamVoice(t, f.udp)
	f.clock.awaitDeadlineCount(t, f.clock.Now().Add(StreamFrameDuration), 2)
	f.clock.advance(StreamFrameDuration + streamPacingCatchupLimit)
	nextStreamVoice(t, f.udp)
	if result := awaitStreamStats(t, done); result.err != nil {
		t.Fatal(result.err)
	}
	second := make(chan StreamFrame, 1)
	second <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	close(second)
	done = startPacedFrames(f.stream, context.Background(), second)
	f.clock.awaitDeadlineCount(t, f.clock.Now().Add(StreamFrameDuration), 2)
	f.clock.advance(39 * time.Millisecond)
	assertNoStreamVoiceWrite(t, f.udp)
	f.clock.advance(time.Millisecond)
	nextStreamVoice(t, f.udp)
	result := awaitStreamStats(t, done)
	if result.err != nil || result.stats.Packets != 1 || !result.stats.FirstWriteAt.Equal(f.clock.Now()) {
		t.Fatalf("new producer inherited old phase: stats=%+v err=%v", result.stats, result.err)
	}
}

func TestStreamAbsolutePacingCancellationDoesNotReleaseQueuedFrame(t *testing.T) {
	f := newStreamingFixture(t, nil)
	frames := make(chan StreamFrame, 2)
	for range cap(frames) {
		frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	}
	close(frames)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startPacedFrames(f.stream, ctx, frames)
	nextStreamVoice(t, f.udp)
	f.clock.awaitDeadlineCount(t, f.clock.Now().Add(StreamFrameDuration), 2)
	cancel()
	result := awaitStreamStats(t, done)
	if !errors.Is(result.err, context.Canceled) || result.stats.Packets != 1 {
		t.Fatalf("pacing cancellation: stats=%+v err=%v", result.stats, result.err)
	}
	f.clock.advance(time.Second)
	assertNoStreamVoiceWrite(t, f.udp)
}

type pacingCostConn struct {
	net.Conn
	clock *manualStreamClock
	cost  time.Duration
}

func (c *pacingCostConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err == nil && len(p) > types.GUIDLength {
		c.clock.advance(c.cost)
	}
	return n, err
}

func TestStreamAbsolutePacingDoesNotAccumulateWriteCost(t *testing.T) {
	f := newStreamingFixture(t, func(o *StreamOptions) {
		original := o.Dial
		o.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := original(ctx, network, address)
			if network == "udp" && err == nil {
				return &pacingCostConn{Conn: conn, clock: o.Clock.(*manualStreamClock), cost: 2 * time.Millisecond}, nil
			}
			return conn, err
		}
	})
	// Register the dispatcher's initial timer before simulated writes move time,
	// keeping it distinct from the transmit deadlines that start two ms later.
	f.clock.awaitDeadline(t, f.clock.Now().Add(StreamFrameDuration))
	firstAt := f.clock.Now().Add(2 * time.Millisecond)
	frames := make(chan StreamFrame, 100)
	for range cap(frames) {
		frames <- StreamFrame{PCM: testTone(StreamFrameSamples)}
	}
	close(frames)
	done := startPacedFrames(f.stream, context.Background(), frames)
	nextStreamVoice(t, f.udp)
	for i := 1; i < 100; i++ {
		target := firstAt.Add(time.Duration(i) * StreamFrameDuration)
		f.clock.awaitDeadline(t, target)
		f.clock.advance(target.Sub(f.clock.Now()) + time.Millisecond)
		nextStreamVoice(t, f.udp)
	}
	result := awaitStreamStats(t, done)
	if result.err != nil || result.stats.Packets != 100 || !result.stats.FirstWriteAt.Equal(firstAt) || result.stats.SumGap != 99*StreamFrameDuration+3*time.Millisecond || result.stats.MaxGap != 43*time.Millisecond {
		t.Fatalf("write cost accumulated across packets: stats=%+v err=%v", result.stats, result.err)
	}
}
