package pt.paradigmshift.babel.antientropy;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import pt.paradigmshift.babel.antientropy.messages.AntiEntropyAnnounce;
import pt.paradigmshift.babel.antientropy.timers.AntiEntropyTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.metrics.Counter;
import pt.unl.fct.di.novasys.babel.metrics.Metric;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.messages.IdentifiableProtoMessage;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.notifications.IdentifiableMessageNotification;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.requests.MissingIdentifiableMessageRequest;
import pt.unl.fct.di.novasys.babel.protocols.general.notifications.ChannelAvailableNotification;
import pt.unl.fct.di.novasys.babel.protocols.membership.notifications.NeighborDown;
import pt.unl.fct.di.novasys.babel.protocols.membership.notifications.NeighborUp;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * Anti-entropy reconciliation protocol for Babel applications.
 *
 * <p>Periodically picks one random connected neighbour, sends it a
 * Bloom-filter summary of the local buffer of identifiable messages, and on
 * receipt of such a summary iterates the local buffer issuing recovery
 * requests for any message the sender's filter does not appear to contain.
 *
 * <h2>Two operating modes</h2>
 *
 * <ul>
 *   <li><b>Self-managed channel:</b> the protocol creates its own
 *       {@link TCPChannel} bound to
 *       {@code AntiEntropy.Channel.Address}:{@code AntiEntropy.Channel.Port}
 *       and opens one out-connection per up-neighbour.</li>
 *   <li><b>Shared channel:</b> if no address/port is configured, the protocol
 *       waits for a {@link ChannelAvailableNotification} and binds to the
 *       shared channel that some other protocol owns. Neighbour-up no longer
 *       triggers a connect — connection management is the channel owner's
 *       responsibility.</li>
 * </ul>
 *
 * <h2>The grace period</h2>
 *
 * Both halves of the protocol respect a symmetric grace period:
 * <ul>
 *   <li>A message that has just been added to the local buffer (younger than
 *       {@code AntiEntropy.GracePeriod}) is excluded from the comparison
 *       against a peer's Bloom filter — the peer has not had time to
 *       acknowledge it yet via the next announce.</li>
 *   <li>A message that is about to be garbage-collected (older than
 *       {@code AntiEntropy.GCTimeout - AntiEntropy.GracePeriod}) is also
 *       excluded — the peer would respond with a message it has just purged
 *       itself, producing an infinite back-and-forth at the edge of the GC
 *       window.</li>
 * </ul>
 *
 * @see AntiEntropyAnnounce
 * @see AntiEntropyTimer
 */
public class AntiEntropy extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(AntiEntropy.class);

    /** Babel protocol numeric identifier. */
    public static final short PROTOCOL_ID = 1900;
    /** Babel protocol name. */
    public static final String PROTOCOL_NAME = "AntiEntropy";

    /** Property key — TCP bind address when the protocol manages its own channel. */
    public static final String PAR_CHANNEL_ADDRESS = "AntiEntropy.Channel.Address";
    /** Property key — TCP bind port when the protocol manages its own channel. */
    public static final String PAR_CHANNEL_PORT = "AntiEntropy.Channel.Port";
    /** Property key — interval between rounds, in milliseconds. */
    public static final String PAR_PERIOD = "AntiEntropy.Period";
    /** Default round interval: 60 seconds. */
    public static final long DEFAULT_PERIOD = 60_000L;
    /** Property key — maximum age of a buffered entry before GC. */
    public static final String PAR_REMOVE_TIME = "AntiEntropy.GCTimeout";
    /** Default GC timeout: 10 minutes. */
    public static final long DEFAULT_REMOVE_TIME = 600_000L;
    /** Property key — Bloom-filter false-positive probability. */
    public static final String PAR_BLOOM_FILTER_FPP = "AntiEntropy.BloomFilter.FPP";
    /** Default Bloom-filter FPP. */
    public static final double DEFAULT_BLOOM_FILTER_FPP = 0.0001d;
    /** Property key — grace period at both ends of the GC window. */
    public static final String PAR_GRACE_PERIOD = "AntiEntropy.GracePeriod";
    /** Default grace period: 90 seconds. */
    public static final long DEFAULT_GRACE_PERIOD = 90_000L;

    /** Entry kept in the local buffer until either reconciliation or GC. */
    private record Element(IdentifiableProtoMessage message, short protocolSource, long timestamp) {
        UUID getMID() {
            return message.getMID();
        }
    }

    private final long antiEntropyPeriod;
    private final long removeTimeWindow;
    private final double bloomFilterFPP;
    private final long gracePeriod;

    private int networkPort;
    private int channelId;
    private Host myself;
    private final boolean managingChannel;

    /** Outbound-connect tier: peers we've heard {@code NeighborUp} for and are connecting to. */
    private final Set<Host> pending = new HashSet<>();
    /** Connected tier: peers we have a confirmed TCP connection to. Iterated by the timer. */
    private final Set<Host> connectedNeighborsSet = new HashSet<>();
    /** Random-access shadow of {@link #connectedNeighborsSet} for O(1) peer sampling. */
    private final List<Host> connectedNeighborsList = new ArrayList<>();

    /**
     * Local buffer of identifiable messages, kept in insertion order. Older
     * messages are at the head; new ones append at the tail. Because local
     * {@link System#currentTimeMillis()} is monotonic per JVM, insertion order
     * is also timestamp order, so the GC sweep can stop at the first
     * young-enough entry.
     *
     * <p>The upstream protocol kept entries in a {@code TreeSet<Element>}
     * compared by timestamp, which silently dropped any two messages that
     * arrived in the same millisecond. Using {@code ArrayDeque} fixes that
     * without losing the cheap ordered-traversal property.
     */
    private final Deque<Element> buffer = new ArrayDeque<>();

    private final Counter sentMessagesCounter;

    /**
     * Construct the protocol. Reads all parameters from {@code properties};
     * either binds its own TCP channel (if address + port are present) or
     * waits for a {@link ChannelAvailableNotification} on a shared channel.
     *
     * @param properties protocol configuration; see the {@code PAR_*} constants
     * @param myself     this node's own {@link Host} identity. May be
     *                   {@code null} if {@code PAR_CHANNEL_ADDRESS} + {@code PAR_CHANNEL_PORT}
     *                   are provided; in that case the protocol will compute
     *                   {@code myself} from those properties.
     */
    public AntiEntropy(Properties properties, Host myself)
            throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        this.antiEntropyPeriod = readLong(properties, PAR_PERIOD, DEFAULT_PERIOD);
        this.removeTimeWindow = readLong(properties, PAR_REMOVE_TIME, DEFAULT_REMOVE_TIME);
        this.gracePeriod = readLong(properties, PAR_GRACE_PERIOD, DEFAULT_GRACE_PERIOD);
        this.bloomFilterFPP = readDouble(properties, PAR_BLOOM_FILTER_FPP, DEFAULT_BLOOM_FILTER_FPP);

        if (gracePeriod * 2 >= removeTimeWindow) {
            logger.warn("GracePeriod ({} ms) is at least half of GCTimeout ({} ms) — "
                    + "the reconciliation window will be empty and the protocol becomes a no-op.",
                    gracePeriod, removeTimeWindow);
        }

        this.sentMessagesCounter = registerMetric(
                new Counter.Builder("SentMessages", Metric.Unit.NONE).build());

        String address = properties.getProperty(PAR_CHANNEL_ADDRESS);
        String port = properties.getProperty(PAR_CHANNEL_PORT);

        if (address != null && port != null) {
            // Self-managed channel branch.
            this.networkPort = Integer.parseInt(port);
            if (this.myself == null) {
                this.myself = new Host(InetAddress.getByName(address), this.networkPort);
            }
        } else if (this.myself != null) {
            address = this.myself.getAddress().getHostAddress();
            this.networkPort = this.myself.getPort();
            port = Integer.toString(this.networkPort);
        }

        if (address != null && port != null) {
            this.managingChannel = true;

            Properties channelProps = new Properties();
            channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
            channelProps.setProperty(TCPChannel.PORT_KEY, port);
            this.channelId = createChannel(TCPChannel.NAME, channelProps);
            setDefaultChannel(this.channelId);

            registerSerializersAndHandlers();
        } else {
            // Shared-channel branch: wait for ChannelAvailableNotification.
            this.managingChannel = false;
            subscribeNotification(ChannelAvailableNotification.NOTIFICATION_ID,
                    this::uponChannelAvailableNotification);
        }

        subscribeNotification(IdentifiableMessageNotification.NOTIFICATION_ID,
                this::uponIdentifiableMessageNotification);
        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUp);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDown);

        registerTimerHandler(AntiEntropyTimer.PROTO_ID, this::uponAntiEntropyTimer);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        setupPeriodicTimer(new AntiEntropyTimer(), antiEntropyPeriod, antiEntropyPeriod);
    }

    /* ────────────────────────────── Timer ────────────────────────────── */

    private void uponAntiEntropyTimer(AntiEntropyTimer timer, long time) {
        // Step 1: announce — pick one neighbour at random and tell it what we have.
        if (!connectedNeighborsList.isEmpty() && !buffer.isEmpty()) {
            Host peer = connectedNeighborsList.get(
                    ThreadLocalRandom.current().nextInt(connectedNeighborsList.size()));

            BloomFilter<String> bf = BloomFilter.create(
                    Funnels.unencodedCharsFunnel(), buffer.size(), bloomFilterFPP);
            for (Element e : buffer) {
                bf.put(e.getMID().toString());
            }

            logger.debug("Sent AntiEntropyAnnounce to {} ({} elements)", peer, buffer.size());
            sendMessage(new AntiEntropyAnnounce(myself, bf, buffer.size()), peer);
            sentMessagesCounter.inc();
        } else if (connectedNeighborsList.isEmpty()) {
            logger.trace("No connected neighbours — skipping announce");
        } else {
            logger.trace("Local buffer empty — skipping announce");
        }

        // Step 2: GC — purge entries older than the configured timeout. Buffer is
        // append-only insertion-order, so we can poll from the head until we hit
        // a young-enough entry.
        long cleanUpBarrier = System.currentTimeMillis() - removeTimeWindow;
        int purged = 0;
        while (!buffer.isEmpty() && buffer.peekFirst().timestamp() < cleanUpBarrier) {
            buffer.pollFirst();
            purged++;
        }
        if (purged > 0) {
            logger.debug("Purged {} elements from local buffer", purged);
        }
    }

    /* ────────────────────────── Message handlers ─────────────────────── */

    private void uponAntiEntropyAnnounceMessage(AntiEntropyAnnounce msg, Host sender, short protoID, int cID) {
        logger.debug("Received AntiEntropyAnnounce reporting {} messages from {}", msg.getSetSize(), sender);

        long now = System.currentTimeMillis();
        long youngerBarrier = now - gracePeriod;
        long olderBarrier = now - removeTimeWindow + gracePeriod;

        for (Element e : buffer) {
            long ts = e.timestamp();
            if (ts >= youngerBarrier) {
                // Too young: peer hasn't had a chance to ack yet.
                continue;
            }
            if (ts <= olderBarrier) {
                // Too old: about to be GC'd. Avoid the edge-of-window cycle.
                continue;
            }
            String mid = e.getMID().toString();
            if (!msg.contains(mid)) {
                logger.info("Requesting recovery of message {} to {}", e.getMID(), sender);
                sendRequest(new MissingIdentifiableMessageRequest(e.message(), sender), e.protocolSource());
            } else {
                logger.trace("Message {} appears known by {}", e.getMID(), sender);
            }
        }
    }

    /* ───────────────────── Notification handlers ─────────────────────── */

    private void uponChannelAvailableNotification(ChannelAvailableNotification event, short protoID) {
        if (myself != null) {
            return; // we already have a channel
        }
        myself = event.getChannelListenData();
        this.networkPort = myself.getPort();
        this.channelId = event.getChannelID();
        registerSharedChannel(this.channelId);

        try {
            registerSerializersAndHandlers();
        } catch (HandlerRegistrationException ex) {
            // A handler-registration failure here means the channel is unusable
            // for this protocol; without it we cannot reconcile, so terminating
            // is the honest behaviour. Logged loudly before exit.
            logger.fatal("Failed to register handlers on shared channel {}", channelId, ex);
            System.exit(1);
        }
    }

    private void uponIdentifiableMessageNotification(IdentifiableMessageNotification event, short protoID) {
        buffer.add(new Element(event.getMessage(), event.getSourceProtocol(),
                System.currentTimeMillis()));
    }

    private void uponNeighborUp(NeighborUp up, short protoID) {
        // Rebind the peer's address to OUR channel's port (the membership protocol
        // may report a different listen port than the one our anti-entropy peer
        // listens on).
        Host h = new Host(up.getPeer().getAddress(), this.networkPort);

        if (managingChannel) {
            if (!connectedNeighborsSet.contains(h) && pending.add(h)) {
                openConnection(h);
            }
        } else {
            // Shared channel: the owner manages connections; we just track membership.
            if (connectedNeighborsSet.add(h)) {
                connectedNeighborsList.add(h);
            }
        }
    }

    private void uponNeighborDown(NeighborDown down, short protoID) {
        Host h = new Host(down.getPeer().getAddress(), this.networkPort);

        if (managingChannel) {
            if (removeConnected(h) || pending.remove(h)) {
                closeConnection(h);
            }
        } else {
            removeConnected(h);
        }
    }

    /* ───────────────────────── Channel events ─────────────────────────── */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host h = event.getNode();
        logger.trace("Host {} is down, cause: {}", h, event.getCause());

        if (connectedNeighborsSet.contains(h) || pending.contains(h)) {
            removeConnected(h);
            pending.add(h);
            openConnection(h);
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<?> event, int channelId) {
        Host h = event.getNode();
        logger.trace("Connection to host {} failed, cause: {}", h, event.getCause());

        if (connectedNeighborsSet.contains(h) || pending.contains(h)) {
            removeConnected(h);
            pending.add(h);
            openConnection(h);
        }
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host h = event.getNode();
        logger.trace("Host (out) {} is up", h);

        if (!connectedNeighborsSet.contains(h) && !pending.contains(h)) {
            // We've stopped caring about this host (membership churn raced us);
            // tear the connection back down to avoid leaking sockets.
            closeConnection(h);
            return;
        }

        pending.remove(h);
        if (connectedNeighborsSet.add(h)) {
            connectedNeighborsList.add(h);
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from host {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* ─────────────────────────── Public access ───────────────────────── */

    /**
     * @return this node's {@link Host}; may be {@code null} if the protocol is
     *         running in shared-channel mode and has not yet received a
     *         {@link ChannelAvailableNotification}
     */
    public Host getHost() {
        return myself;
    }

    /* ────────────────────────────── Helpers ──────────────────────────── */

    private void registerSerializersAndHandlers() throws HandlerRegistrationException {
        registerMessageSerializer(channelId, AntiEntropyAnnounce.MSG_CODE, AntiEntropyAnnounce.serializer);
        registerMessageHandler(channelId, AntiEntropyAnnounce.MSG_CODE, this::uponAntiEntropyAnnounceMessage);

        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
    }

    private boolean removeConnected(Host h) {
        boolean wasIn = connectedNeighborsSet.remove(h);
        if (wasIn) {
            connectedNeighborsList.remove(h);
        }
        return wasIn;
    }

    private static long readLong(Properties p, String key, long defaultValue) {
        String v = p.getProperty(key);
        return v == null ? defaultValue : Long.parseLong(v);
    }

    private static double readDouble(Properties p, String key, double defaultValue) {
        String v = p.getProperty(key);
        return v == null ? defaultValue : Double.parseDouble(v);
    }
}
