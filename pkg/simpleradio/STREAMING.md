# Incremental SRS transport

`ConnectStream` is an explicit alternative to the original whole-utterance
`Client` API. The legacy API is unchanged. The streaming runtime establishes TCP
and UDP connections, sends synchronization and External AWACS authentication,
waits for an authentication reply, then exposes a `Stream`. It uses the existing
SRS wire messages and voice packet codec; no application/controller logic is
included. Official SRS-client interoperability remains a separate integration gate.

## Audio and ownership

Audio is mono at 16,000 Hz. Each packet represents 40 milliseconds: 640 normalized
float32 PCM samples, or one equivalent Opus packet. `Transmit` accepts exactly one
encoding per frame. Received audio events contain both PCM and original Opus.
A received buffer belongs exclusively to its consumer. Sending a frame transfers
its buffers to `Transmit`; a producer must not mutate or reuse them afterward.
No implicit resampling, channel conversion, final-frame padding, FEC, concealment
or speech recognition occurs. Producers must explicitly convert and pad audio.

`Transmit(ctx, frames)` begins with the first available frame, while the producer
can continue generating later frames. Use an unbuffered or bounded input channel.
The runtime retains at most one pending transmit frame and spaces all outgoing
voice packets by at least 40 ms, including across adjacent transmit calls. One
producer is admitted at a time (`ErrStreamBusy`). A producer silence exceeding
`Underrun` aborts that call (`ErrStreamUnderrun`); it does not transmit silence or
resume abandoned output. Cancel the producer as well when abandoning an utterance.
Cancellation interrupts pacing, queue waits and network writes. UDP writes have
context-aware deadlines; TCP initialization and writes have connection deadlines.

## Reception and routing

`Receive` publishes a `StreamAudio` event immediately for each accepted packet,
before the transmission ends. A `StreamGap` event ends its transmission after
`Gap` without new accepted audio. Gap expiration is checked at 40 ms intervals,
so a timer-only completion may arrive up to one frame after the configured gap.
Reception processes packet arrival even if an expiration tick has not run yet.
A gap completion is distinct from connection termination: `Receive` closes on
termination, and `Err` reports network disconnection, authentication failure,
overflow or cancellation. It never reports a disconnect as normal gap completion.

Metadata retains the original and relay GUIDs separately, raw unit ID, packet
sequence, hops and every packet frequency/modulation/encryption selector. Frequencies
must be finite positive integral hertz. Reception requires an exact configured
frequency, modulation and encryption-selector match. An encrypted channel must
configure its nonzero encryption key. Implicit guard-frequency and retransmit
configuration are rejected; represent each listening channel as an explicit radio.
This API does not invent decryption/range/visibility claims. Coalition is absent from UDP and is marked unknown unless TCP supplied
that origin's coalition. Coalition security defaults on until the server supplies
its setting. Unknown origins are withheld when coalition security is enabled.
Zero and the external-AWACS unit sentinel are marked unavailable for aircraft
correlation. Transport identity never establishes a spoken callsign or GCI identity.

Overlapping origins have independent decoder/transmission state. Lost packet
numbers are counted within an active transmission; missing audio is skipped.
Late/reordered packets are discarded immediately, and duplicates/relays are
suppressed by the original GUID plus sequence high-watermark. High-watermarks
survive gap completion for the lifetime of the connection, preventing delayed
relay packets from reopening old utterances. Changing origin GUID or reconnecting
starts a fresh epoch. A remote sender that resets its sequence under the same GUID
requires a fresh local connection; automatically accepting that reset would admit
stale relays. Application mission/aircraft binding remains the consumer's
responsibility.

## Bounds and lifecycle

`StreamOptions` supplies bounded receive events, datagram queue, distinct origin
state and TCP peer counts. Defaults are 64 receive events, 64 packets, 128 origins
and 1,024 peers; configurable maxima are validated before dialing. TCP lines are
limited to 256 KiB, UDP datagrams to less than 1,500 bytes, and audio frames to the
fixed format. Exhausting an event, packet, peer or origin bound terminates the
connection visibly with `ErrStreamQueueFull`; it never silently evicts duplicate
history or buffers an unbounded utterance. Queue termination discards pending
received audio before closing the event stream.

`Close` is idempotent and interrupts pending network reads/writes. `Done` closes
after all session readers, ping service and receive dispatcher stop. Explicit
close and parent cancellation report `context.Canceled`; a TCP/UDP read failure
reports `ErrStreamDisconnected`. Reconnection is explicit: close the old stream,
then call `ConnectStream` again. It generates a fresh GUID and empty peer/audio
state. There is no automatic retry, worker supervisor or replay of prior output.

Credentials are injected only at the connection boundary and used for the
External AWACS handshake. The runtime retains no credential in its configuration,
emits no raw protocol/network logs, and returns fixed errors rather than payloads
or network error strings. `Dial` and `StreamClock` permit deterministic fixtures;
the normal implementation uses `net.Dialer` and the wall clock. Test fixtures use
synthetic PCM/Opus, in-memory TCP and datagram boundaries and a manual clock.
They demonstrate transport behavior, not compatibility with an official SRS
client, live DCS, Windows support or deployment/load/long-running behavior.

Run the transport suite from the fork with its native Opus prerequisites:

```sh
GOWORK=off go test -race ./pkg/simpleradio/...
GOWORK=off go vet ./pkg/simpleradio/...
```
