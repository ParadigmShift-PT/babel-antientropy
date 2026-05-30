package pt.paradigmshift.babel.antientropy.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * Periodic timer that fires one anti-entropy round in
 * {@link pt.paradigmshift.babel.antientropy.AntiEntropy}.
 *
 * <p>Handler class: timer. <b>ID:</b> {@value #PROTO_ID}. Owning protocol:
 * {@code AntiEntropy} (id 1900).
 */
public class AntiEntropyTimer extends ProtoTimer {

    /** Babel timer numeric identifier. */
    public static final short PROTO_ID = 1901;

    public AntiEntropyTimer() {
        super(PROTO_ID);
    }

    /**
     * Stateless timer: returns {@code this} rather than allocating a new instance.
     * See workspace memory note "Babel timer clone()".
     */
    @Override
    public ProtoTimer clone() {
        return this;
    }
}
