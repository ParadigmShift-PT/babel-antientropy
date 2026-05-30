package pt.paradigmshift.babel.antientropy.messages;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * One-shot announce sent by {@link pt.paradigmshift.babel.antientropy.AntiEntropy}
 * to a single random neighbour every {@code AntiEntropy.Period} ms.
 *
 * <p>Wire content: the sender's {@link Host}, the local buffer's element count
 * (informational — the Bloom filter itself does not expose its element count
 * once built), and the Bloom filter that summarises the local set of message
 * identifiers.
 *
 * <p>The Bloom filter is wire-encoded with Guava's native
 * {@link BloomFilter#writeTo(java.io.OutputStream)} compact format. The
 * upstream protocol used Java {@code ObjectOutputStream} here, which is both
 * larger on the wire and a well-known deserialisation-attack vector when
 * decoding bytes from an untrusted peer.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}. Owning protocol:
 * {@code AntiEntropy} (id 1900).
 */
public class AntiEntropyAnnounce extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 1901;

    private final Host sender;
    private final int setSize;
    private final BloomFilter<String> receivedMessages;

    public AntiEntropyAnnounce(Host sender, BloomFilter<String> bf, int setSize) {
        super(AntiEntropyAnnounce.MSG_CODE);
        this.sender = sender;
        this.receivedMessages = bf;
        this.setSize = setSize;
    }

    public Host getSender() {
        return sender;
    }

    /**
     * @return the cardinality of the sender's local buffer at the moment the
     *         filter was built; informational only, useful for metrics
     */
    public int getSetSize() {
        return setSize;
    }

    /**
     * @param id message identifier (as string)
     * @return {@code true} if the sender's filter likely contains {@code id}
     *         (subject to the configured false-positive probability)
     */
    public boolean contains(String id) {
        return receivedMessages.mightContain(id);
    }

    @Override
    public String toString() {
        return "AntiEntropyAnnounce{sender=" + sender + ", setSize=" + setSize + "}";
    }

    public static final ISerializer<AntiEntropyAnnounce> serializer = new ISerializer<>() {

        @Override
        public void serialize(AntiEntropyAnnounce t, ByteBuf out) throws IOException {
            Host.serializer.serialize(t.getSender(), out);
            out.writeInt(t.setSize);

            // Guava's native compact format. We encode into a byte[] first so we can
            // length-prefix it (the writeTo stream offers no length up front).
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            t.receivedMessages.writeTo(baos);
            byte[] encoded = baos.toByteArray();
            out.writeInt(encoded.length);
            out.writeBytes(encoded);
        }

        @Override
        public AntiEntropyAnnounce deserialize(ByteBuf in) throws IOException {
            Host h = Host.serializer.deserialize(in);
            int setSize = in.readInt();
            int len = in.readInt();
            if (len < 0 || len > in.readableBytes()) {
                throw new IOException("AntiEntropyAnnounce: bloom-filter length out of range: " + len);
            }
            byte[] encoded = new byte[len];
            in.readBytes(encoded);
            BloomFilter<String> filter;
            try (ByteArrayInputStream bais = new ByteArrayInputStream(encoded)) {
                filter = BloomFilter.readFrom(bais, Funnels.unencodedCharsFunnel());
            }
            return new AntiEntropyAnnounce(h, filter, setSize);
        }
    };
}
