package utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class BasicUtils {

    private static final int MIN_BATCH = 6;
    private static final int MAX_BATCH = 27;
    private static final int ADDED_LIMIT = 500000;
    private static final int DELETED_LIMIT = 200000;

    private static ConcurrentHashMap ringsNotToDelete = new ConcurrentHashMap();

    public static int getMaxBatch() {
        return MAX_BATCH;
    }

    public static int getAddedLimit() {
        return ADDED_LIMIT;
    }

    public static int getDeletedLimit() {
        return DELETED_LIMIT;
    }

    public static int generateBatchSize() {
        return ThreadLocalRandom.current().nextInt(MIN_BATCH, MAX_BATCH);
    }

    public static long generateRnd() {
        return ThreadLocalRandom.current().nextLong(0, Counter.getVertexesNumber() * 1000);
    }

    public static long getRandomVertexId() {
        return ThreadLocalRandom.current().nextLong(1, Counter.getVertexesNumber());
    }

    public static void keepRing(long ringId) {
        ringsNotToDelete.put(ringId, true);
    }

    public static void allowDeleteRing(long ringId) {
        ringsNotToDelete.remove(ringId);
    }

    public static boolean containsAndSetRingId(long ringId) {
        return ringsNotToDelete.putIfAbsent(ringId, true) != null;
    }
}
