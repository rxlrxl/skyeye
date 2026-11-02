package simpleradio

import (
	"reflect"
	"testing"
	"time"

	"github.com/dharmab/skyeye/pkg/coalitions"
	"github.com/dharmab/skyeye/pkg/simpleradio/types"
)

func TestStreamTCPMembershipLossRevokesAdmittedSenderWithoutAnotherPacket(t *testing.T) {
	for _, completed := range []bool{false, true} {
		for _, kind := range []string{"disconnect", "sync_withdrawal", "coalition_change"} {
			name := kind + "_active_input"
			if completed {
				name = kind + "_completed_input"
			}
			t.Run(name, func(t *testing.T) {
				f := newStreamingFixture(t, nil) // Coalition security is false.
				sender := types.GUID("1111111111111111111111")
				other := types.GUID("2222222222222222222222")
				peer := types.ClientInfo{GUID: sender, Coalition: coalitions.Blue}
				f.serverMessage(t, types.Message{Type: types.MessageUpdate, Client: &peer})
				audio := streamFixtureAudio(t)
				f.inject(streamFixturePacket(audio, sender, 1))
				first := nextStreamEvent(t, f.stream)
				if first.Kind != StreamAudio {
					t.Fatal("missing admitted input")
				}
				if completed {
					f.clock.advance(320 * time.Millisecond)
					if gap := nextStreamEvent(t, f.stream); gap.Kind != StreamGap {
						t.Fatal("missing input completion")
					}
				}
				switch kind {
				case "disconnect":
					f.serverMessage(t, types.Message{Type: types.MessageClientDisconnect, Client: &types.ClientInfo{GUID: sender}})
				case "sync_withdrawal":
					f.serverMessage(t, types.Message{Type: types.MessageSync})
				case "coalition_change":
					f.serverMessage(t, types.Message{Type: types.MessageRadioUpdate, Client: &types.ClientInfo{GUID: sender, Coalition: coalitions.Red}})
				}
				// No further UDP packet, gap timeout or transmit activity causes
				// this event; it comes directly from the TCP membership change.
				departure := nextStreamEvent(t, f.stream)
				want := StreamEvent{Kind: StreamSenderDisconnected, Metadata: StreamMetadata{OriginGUID: sender}}
				if !reflect.DeepEqual(departure, want) {
					t.Fatalf("departure retained audio or stale metadata: %+v", departure)
				}
				// Retired senders cannot rearm from delayed UDP even when ordinary
				// unknown senders are allowed by disabled coalition security.
				f.inject(streamFixturePacket(audio, sender, 2))
				f.inject(streamFixturePacket(audio, other, 1))
				if event := nextStreamEvent(t, f.stream); event.Kind != StreamAudio || event.Metadata.OriginGUID != other {
					t.Fatalf("retired sender replayed delayed audio: %+v", event)
				}
				// A fresh peer update permits a new transmission, with sequence
				// high watermarks preserved across departure and decoder disposal.
				f.serverMessage(t, types.Message{Type: types.MessageUpdate, Client: &peer})
				f.inject(streamFixturePacket(audio, sender, 1))
				f.inject(streamFixturePacket(audio, sender, 3))
				fresh := nextStreamEvent(t, f.stream)
				if fresh.Kind != StreamAudio || fresh.Metadata.OriginGUID != sender || fresh.Metadata.PacketID != 3 || fresh.TransmissionID == first.TransmissionID {
					t.Fatalf("fresh sender failed to recover independently: %+v", fresh)
				}
			})
		}
	}
}

func TestStreamUnadmittedDepartureProducesNoControlInput(t *testing.T) {
	f := newStreamingFixture(t, nil)
	unknown := types.GUID("1111111111111111111111")
	admitted := types.GUID("2222222222222222222222")
	f.serverMessage(t, types.Message{Type: types.MessageClientDisconnect, Client: &types.ClientInfo{GUID: unknown}})
	f.inject(streamFixturePacket(streamFixtureAudio(t), admitted, 1))
	if event := nextStreamEvent(t, f.stream); event.Kind != StreamAudio || event.Metadata.OriginGUID != admitted {
		t.Fatalf("unadmitted peer created a control input: %+v", event)
	}
}
