# Babel Anti-Entropy

Anti-entropy reconciliation protocol for [Babel](https://github.com/) applications.
Provided and evolved independently of the original work.

**Group ID:** `pt.paradigmshift.babel`
**Artifact ID:** `broadcast-antientropy`
**Current version:** `0.1.0`
**Tested with:** `pt.paradigmshift.babel:babel-core` (Babel-Swarm core fork) and
`pt.paradigmshift.babel:babel-protocols-common` (the shared dissemination / membership API surface).
**Source / target:** Java 17.

---

## What it does

Anti-entropy is the safety net underneath any unreliable dissemination protocol.
The dissemination layer (e.g. an eager-push gossip protocol) optimises for low
latency and accepts some message loss; anti-entropy runs periodically in the
background and reconciles state between peers so that — eventually — every node
ends up with the same set of messages.

The reconciliation step uses a compact Bloom-filter summary instead of an
explicit message-ID list, so the announcement is small even for very long
buffers. False positives are tunable via the `BloomFilter.FPP` parameter; a
false positive merely means a message that *is* known at the peer is skipped
in this round and retried next round — no correctness impact.

### One round, end-to-end

1. **Local announce** — every `Period` ms each node picks one random connected
   neighbour, builds a Bloom filter over the IDs of its current local buffer,
   and sends an `AntiEntropyAnnounce` to that neighbour.
2. **Peer compares** — on receipt of the announce, the peer iterates its own
   buffer; for each local message that has aged past the *grace period* but
   is still inside the GC window, it consults the Bloom filter. If the sender
   *probably does not* have that message, the peer fires a
   `MissingIdentifiableMessageRequest` toward the originating dissemination
   protocol (which is then responsible for delivering the message to the
   sender via its own channel).
3. **Local GC** — the timer also evicts any buffered entry older than the GC
   window, keeping the buffer bounded.

The grace period at both ends keeps the protocol from oscillating: a node
will not re-send a message that has just arrived at it (the receiver has not
had time to acknowledge yet via its next announce), and a node will not
request recovery of a message that is about to be garbage-collected
(closes a window where the sender would respond with a message it has just
purged itself).

## Protocol & event identifiers

This module follows the Babel ID convention used across the ParadigmShift
workspace: protocol IDs at 100-multiples; events numbered per handler class
from `protocol_id + 1` upward, four independent pools (notifications,
messages, requests+replies, timers).

`AntiEntropy` claims **protocol slot 1900**.

| Type | Handler class | ID | Purpose |
|---|---|---|---|
| `AntiEntropyAnnounce` | message | `1901` | Bloom-filter summary of the local buffer sent to one random neighbour per period |
| `AntiEntropyTimer`    | timer   | `1901` | Periodic tick that triggers an announce + a GC sweep |

## Configuration

| Property | Default | Description |
|---|---|---|
| `AntiEntropy.Channel.Address` | — | TCP bind address when running this protocol with its own channel. If unset, the protocol expects a shared channel via `ChannelAvailableNotification`. |
| `AntiEntropy.Channel.Port`    | — | TCP bind port (paired with the above). |
| `AntiEntropy.Period`          | `60000` ms | Interval between anti-entropy rounds. |
| `AntiEntropy.GCTimeout`       | `600000` ms | Maximum age of a buffered entry before it is garbage-collected locally. |
| `AntiEntropy.GracePeriod`     | `90000` ms | Both ends of the protective window described above. Must be smaller than `GCTimeout / 2`. |
| `AntiEntropy.BloomFilter.FPP` | `0.0001` | False-positive probability for the Bloom filter. Smaller values mean larger announces. |

## How application protocols plug in

`AntiEntropy` is dissemination-agnostic. Any protocol that exposes
`IdentifiableProtoMessage`s, fires `IdentifiableMessageNotification` when it
delivers them locally, and handles `MissingIdentifiableMessageRequest` to
recover a missing message can use anti-entropy as a reconciliation layer.

Membership comes from any protocol that fires `NeighborUp` /
`NeighborDown` — typically HyParView or a static-peer list wrapper. The
protocol opens its own outbound TCP connection to each up-neighbour
(symmetric to inbound).

## Build

```bash
mvn clean install
```

The protocol depends on `babel-sc-core` and `babel-protocol-commons-j21` from
the NOVA SYS Maven repository (`https://novasys.di.fct.unl.pt/packages/mvn`).
The repository is listed in `pom.xml`; no extra Maven configuration is needed.

## Tuning notes

- `Period` × number-of-peers should be **less than** the eager-push round-trip
  time the dissemination layer can sustain; otherwise anti-entropy will
  request retransmission of messages that are still in flight on the eager
  path.
- `GCTimeout` must be larger than the maximum tolerable disconnect window: if
  a peer is offline longer than `GCTimeout`, items the rest of the mesh
  reconciled will be lost to that peer (since they'll have aged out of every
  buffer by the time the peer rejoins).
- `BloomFilter.FPP` interacts with buffer size — the announce grows roughly
  as `-1.44 × ln(FPP) × bufferSize` bits. At the default `0.0001` FPP, a
  10 000-entry buffer produces a ~24 KiB announce.

## Differences from the upstream

This is a ParadigmShift evolution of the original protocol. Headline changes:

- The "AntiEntrophy"/"AntiEntophy" typos in package, class, message and
  property names are corrected to "AntiEntropy".
- Maven coordinates moved to `pt.paradigmshift.babel:broadcast-antientropy`.
- Bloom filter wire format uses Guava's native `writeTo` / `readFrom`
  rather than Java `ObjectOutputStream` — smaller payload and not subject
  to the Java-serialization deserialization risk.
- A malformed `AntiEntropyAnnounce` no longer terminates the JVM (the
  original called `System.exit(1)` on `ClassNotFoundException`).
- The Bloom filter is no longer constructed when the local buffer is
  empty (Guava throws `IllegalArgumentException` on
  `expectedInsertions == 0`).
- Buffered entries are no longer indexed by a `Long` timestamp via
  `TreeSet` (which silently dropped multiple messages arriving in the
  same millisecond); they are kept in an `ArrayDeque` ordered by
  insertion order, which is also strictly older-first because timestamps
  are monotonic per local clock.
- Network port parsing widened from `Short.parseShort` (ports > 32 767
  threw `NumberFormatException`) to `Integer.parseInt`.
- Random neighbour selection is O(1) via an additional `ArrayList`
  shadow of the connected-neighbour set.
- Public javadoc on every public type and method.
- Public mutable parameter fields demoted to `private final` with
  package-private getters where the integration tests need them.
