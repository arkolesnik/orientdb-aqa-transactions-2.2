package utils;

import java.util.concurrent.atomic.AtomicLong;

public class Counter {

    private static AtomicLong vertexCounter = new AtomicLong(0);
    private static AtomicLong deletedVertexCounter = new AtomicLong(0);

    private static AtomicLong ringCounter = new AtomicLong(0);

    public static long getNextVertexId() {
        return vertexCounter.incrementAndGet();
    }

    public static long getVertexesNumber() {
        return vertexCounter.get();
    }

    public static void incrementDeleted() {
        deletedVertexCounter.incrementAndGet();
    }

    public static long getDeleted() {
        return deletedVertexCounter.get();
    }

    public static long getNextRingId() {
        return ringCounter.incrementAndGet();
    }

    public static long getInstanceNumber() {
        return vertexCounter.get();
    }

}
