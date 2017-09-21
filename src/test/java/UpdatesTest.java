import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import utils.BasicUtils;
import utils.Counter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class UpdatesTest extends CreateGraphDatabaseFixture {

    private static final String VERTEX_CLASS = "TestVertexClass";
    private static final String VERTEX_ID = "vertexId";
    private static final String CREATOR_IDS = "creatorIds";
    private static final String BATCH_COUNTS = "batchCounts";
    private static final String ITERATIONS = "iterations";
    private static final String RND = "rnd";
    private static final String RING_IDS = "ringIds";
    private static final String EDGE_CLASS = "connects";
    private static final String EDGE_RING_ID = "ringId";

    private static final Logger LOG = LoggerFactory.getLogger(UpdatesTest.class);

    @Test
    public void shouldCheckUpdates() throws InterruptedException, ExecutionException {
        OrientGraphNoTx graphNoTx = factory.getNoTx();
        OClass vertexClass = graphNoTx.createVertexType(VERTEX_CLASS);
        OClass edgeClass = graphNoTx.createEdgeType(EDGE_CLASS);
        createVertexProperties(vertexClass);
        createEdgeProperties(edgeClass);
        createIndexes(vertexClass);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Callable<Object>> tasksToCreate = new ArrayList<>();
        List<Callable<Object>> tasksToUpdate = new ArrayList<>();
        List<Callable<Object>> tasksToRead = new ArrayList<>();

        try {
            for (int i = 0; i < 4; i++) {
                tasksToCreate.add(() -> {
                    long iterationNumber = 0;
                    OrientGraph graph;
                    try {
                        graph = factory.getTx();
                        while (Counter.getVertexesNumber() < BasicUtils.getAddedLimit()) {
                            iterationNumber++;
                            addVertexesAndEdges(graph, iterationNumber);
                        }
                        LOG.info("Graph shutdown by creating thread " + Thread.currentThread().getId());
                        graph.shutdown();
                        return null;
                    } catch (Exception e) {
                        LOG.error("Exception during operation processing", e);
                        throw e;
                    }
                });
            }
            for (int i = 0; i < 2; i++) {
                tasksToUpdate.add(() -> {
                    long iterationNumber = 0;
                    OrientGraph graph;
                    try {
                        graph = factory.getTx();
                        while (Counter.getRingsCounter() < Counter.getVertexesNumber() / 5) {
                            iterationNumber++;
                            updateVertexesAndEdges(graph, iterationNumber);
                        }
                        LOG.info("Graph shutdown by updating thread " + Thread.currentThread().getId());
                        graph.shutdown();
                        return null;
                    } catch (Exception e) {
                        LOG.error("Exception during operation processing", e);
                        throw e;
                    }
                });
            }
            for (int i = 0; i < 2; i++) {
                tasksToRead.add(() -> {
                    long iterationNumber = 0;
                    OrientGraph graph;
                    try {
                        graph = factory.getTx();
                        //TODO: change this condition
                        while (Counter.getVertexesNumber() > BasicUtils.getMaxBatch()
                                && Counter.getInstanceNumber() > 100) {
                            iterationNumber++;
                            readAllRings(graph, iterationNumber);
                        }
                        LOG.info("Graph shutdown by reading thread " + Thread.currentThread().getId());
                        graph.shutdown();
                        return null;
                    } catch (Exception e) {
                        LOG.error("Exception during operation processing", e);
                        throw e;
                    }
                });
            }

            tasksToCreate.addAll(tasksToUpdate);
            tasksToCreate.addAll(tasksToRead);

            List<Future<Object>> futures = executor.invokeAll(tasksToCreate);
            for (Future future : futures) {
                future.get();
            }
        } finally {
            executor.shutdown();
        }
    }

    private void createVertexProperties(OClass vertexClass) {
        vertexClass.createProperty(VERTEX_ID, OType.LONG);
        vertexClass.createProperty(CREATOR_IDS, OType.EMBEDDEDLIST, OType.LONG);
        vertexClass.createProperty(BATCH_COUNTS, OType.EMBEDDEDLIST, OType.INTEGER);
        vertexClass.createProperty(ITERATIONS, OType.EMBEDDEDLIST, OType.LONG);
        vertexClass.createProperty(RND, OType.LONG);
        vertexClass.createProperty(RING_IDS, OType.EMBEDDEDLIST, OType.LONG);
    }

    private void createEdgeProperties(OClass edgeClass) {
        edgeClass.createProperty(EDGE_RING_ID, OType.LONG);
    }

    private void createIndexes(OClass clazz) {
        clazz.createIndex(VERTEX_CLASS + "." + VERTEX_ID, OClass.INDEX_TYPE.NOTUNIQUE, VERTEX_ID);
        clazz.createIndex(VERTEX_CLASS + "." + RND, OClass.INDEX_TYPE.UNIQUE, RND);
        clazz.createIndex(VERTEX_CLASS + "." + CREATOR_IDS + "_" + VERTEX_ID,
                OClass.INDEX_TYPE.NOTUNIQUE, CREATOR_IDS, VERTEX_ID);
    }

    private void addVertexesAndEdges(OrientGraph graph, long iterationNumber) {
        int batchCount = BasicUtils.generateBatchSize();
        List<Vertex> vertexes = new ArrayList<>(batchCount);
        List<Long> ids = new ArrayList<>();
        long ringId = Counter.getNextRingId();
        long threadId = Thread.currentThread().getId();

        List<Long> threadIds = new ArrayList<>();
        threadIds.add(threadId);

        List<Integer> batchCounts = new ArrayList<>();
        batchCounts.add(batchCount);

        List<Long> iterations = new ArrayList<>();
        iterations.add(iterationNumber);

        List<Long> ringIds = new ArrayList<>();
        ringIds.add(ringId);

        try {
            for (int i = 0; i < batchCount; i++) {
                Vertex vertex = graph.addVertex("class:" + VERTEX_CLASS);
                long vertexId = Counter.getNextVertexId();
                ids.add(vertexId);
                vertex.setProperty(VERTEX_ID, vertexId);
                vertex.setProperty(CREATOR_IDS, threadIds);
                vertex.setProperty(BATCH_COUNTS, batchCounts);
                vertex.setProperty(ITERATIONS, iterations);
                vertex.setProperty(RND, BasicUtils.generateRnd());
                vertex.setProperty(RING_IDS, ringIds);

                vertexes.add(vertex);
                //TODO: duplicated part
                int addedVertexes = vertexes.size();

                if (addedVertexes > 1) {
                    Edge edge = graph.addEdge(null, vertexes.get(i - 1), vertexes.get(i), EDGE_CLASS);
                    edge.setProperty(EDGE_RING_ID, ringId);
                }
                if (addedVertexes == batchCount) {
                    Edge edge = graph.addEdge(null, vertexes.get(i), vertexes.get(0), EDGE_CLASS);
                    edge.setProperty(EDGE_RING_ID, ringId);
                }
            }
            BasicUtils.keepRing(ringId);
            graph.commit();
            LOG.info("Ring " + ringId + " was created by thread " + threadId);

            //actions after commit
            performSelectOperations(graph, ids, iterationNumber, threadId, ids.size(), 1);
            checkRingCreated(graph, ids);
        } catch (ORecordDuplicatedException e) {
            LOG.error("Duplicated record", e);
            graph.rollback();
            //actions after rollback
            performSelectOperations(graph, ids, iterationNumber, threadId, 0, 0);
        } catch (Exception e) {
            LOG.error("Exception was caught");
            graph.rollback();
            throw e;
        }
    }

    private void performSelectOperations(OrientGraph graph,
                                         List<Long> ids,
                                         long iteration,
                                         long threadId,
                                         int expectedAll,
                                         int expectedUnique) {

        selectAll(graph, ids, iteration, threadId, expectedAll);
        selectByIds(graph, ids, expectedUnique);
    }

    private void selectAll(OrientGraph graph, List<Long> ids, long iteration, long threadId, int expectedAll) {
        long firstId = ids.get(0);
        long lastId = ids.get(ids.size() - 1);
        long limit = lastId - firstId + 1;

        OrientDynaElementIterable allRecords = graph.command(new OCommandSQL(
                "select * from V where vertexId <= " + lastId + " and iterations contains " + iteration
                        + " and creatorIds contains " + threadId + " order by vertexId limit " + limit))
                .execute();

        int recordsNumber = getRecordsNumber(allRecords);

        Assert.assertEquals(recordsNumber, expectedAll,
                "Selecting of all vertexes returned a wrong number of records, # of ids " + ids.size());
    }

    private void selectByIds(OrientGraph graph, List<Long> ids, int expectedUnique) {
        for (long id : ids) {
            OrientDynaElementIterable uniqueItem
                    = graph.command(new OCommandSQL("select from V where " + VERTEX_ID + " = " + id)).execute();

            int recordsNumber = getRecordsNumber(uniqueItem);

            Assert.assertEquals(recordsNumber, expectedUnique,
                    "Selecting by vertexId returned a wrong number of records");
        }
    }

    private int getRecordsNumber(OrientDynaElementIterable records) {
        int recordsNumber = 0;
        while (records.iterator().hasNext()) {
            records.iterator().next();
            recordsNumber++;
        }
        return recordsNumber;
    }

    private void checkRingCreated(OrientGraph graph, List<Long> ids) {
        List<Long> creatorIdsFromVertexes = new ArrayList<>();
        List<Long> iterationsFromVertexes = new ArrayList<>();

        long firstVertexId = ids.get(0);

        OrientDynaElementIterable result
                = graph.command(new OCommandSQL("select from V where " + VERTEX_ID + " = " + firstVertexId))
                .execute();

        Vertex vertex = null;
        if (result.iterator().hasNext()) {
            vertex = (OrientVertex) result.iterator().next();
        } else {
            LOG.warn(firstVertexId + " vertexId wasn't selected");
        }

        List<Integer> batchCounts = vertex.getProperty(BATCH_COUNTS);
        int batchCount = batchCounts.get(batchCounts.size() - 1);
        List<Long> ringIds = vertex.getProperty(RING_IDS);
        long ringId = ringIds.get(ringIds.size() - 1);

        for (int i = 0; i < batchCount; i++) {
            List<Long> creatorIds = vertex.getProperty(CREATOR_IDS);
            creatorIdsFromVertexes.add(creatorIds.get(creatorIds.size() - 1));
            List<Long> iterations = vertex.getProperty(ITERATIONS);
            iterationsFromVertexes.add(iterations.get(iterations.size() - 1));
            Iterable<Edge> edges = vertex.getEdges(Direction.OUT, EDGE_CLASS);
            Assert.assertTrue(edges.iterator().hasNext(),
                    "Edge OUT doesn't exist in vertex " + vertex.getProperty(VERTEX_ID));

            Edge edge = null;
            while (edges.iterator().hasNext()) {
                Edge currentEdge = edges.iterator().next();
                if (currentEdge.getProperty(EDGE_RING_ID).equals(ringId)) {
                    edge = currentEdge;
                    break;
                }
            }
            Vertex nextVertex = edge.getVertex(Direction.IN);
            long vertexId;
            if (i == batchCount - 1) {
                vertexId = ids.get(0);
            } else {
                vertexId = ids.get(i + 1);
            }

            boolean connected = nextVertex.getProperty(VERTEX_ID).equals(vertexId);
/*            Assert.assertTrue(connected, "Vertexes are not correctly connected by edges: vertex "
                    + nextVertex.getProperty(VERTEX_ID) + " and vertex " + vertexId);*/
            if (!connected) {
                LOG.error("Vertexes are NOT correctly connected by edges: vertex "
                        + nextVertex.getProperty(VERTEX_ID) + " and vertex " + vertexId + ", ring "
                        + ringId);
            }
            vertex = nextVertex;

        }
        boolean isOneThread = creatorIdsFromVertexes.stream().distinct().limit(2).count() <= 1;
        boolean isOneIteration = iterationsFromVertexes.stream().distinct().limit(2).count() <= 1;
        Assert.assertTrue(isOneThread, "Vertexes are not created by one thread");
        Assert.assertTrue(isOneIteration, "Vertexes are not created during one iteration");
    }

    private Vertex randomlySelectVertex() {
        OrientGraph graph = factory.getTx();
        Vertex selectedVertex;
        long randomId;
        while (true) {
            randomId = ThreadLocalRandom.current().nextLong(1, Counter.getInstanceNumber());
            OrientDynaElementIterable result = graph.command(
                    new OCommandSQL("select from " + VERTEX_CLASS + " where "
                            + VERTEX_ID + " = " + randomId)).execute();
            if (result.iterator().hasNext()) {
                selectedVertex = (Vertex) result.iterator().next();
                break;
            }
        }
        return selectedVertex;
    }

    private void updateVertexesAndEdges(OrientGraph graph, long iterationNumber) {
        //int batchCount = BasicUtils.generateBatchSize();
        int batchCount = 4;
        List<Vertex> vertexes = new ArrayList<>();
        List<Long> ids = new ArrayList<>();
        long ringId = Counter.getNextRingId();
        for (int i = 0; i < batchCount; i++) {
            Vertex vertex;
            while (true) {
                vertex = randomlySelectVertex();
                List<Long> ringIds = vertex.getProperty(RING_IDS);
                if (ringIds.size() < 100) { //extract this in the constant
                    break;
                } else {
                    Counter.incrementRingsCounter();
                }
            }
            ids.add(vertex.getProperty(VERTEX_ID));
            List<Long> creatorIds = vertex.getProperty(CREATOR_IDS);
            creatorIds.add(Thread.currentThread().getId());
            List<Integer> batchCounts = vertex.getProperty(BATCH_COUNTS);
            batchCounts.add(batchCount);
            List<Long> iterations = vertex.getProperty(ITERATIONS);
            iterations.add(iterationNumber);
            List<Long> ringIds = vertex.getProperty(RING_IDS);
            ringIds.add(ringId);

            vertexes.add(vertex);

            //TODO: duplicated part
            int addedVertexes = vertexes.size();
            if (addedVertexes > 1) {
                Edge edge = graph.addEdge(null, vertexes.get(i - 1), vertexes.get(i), EDGE_CLASS);
                edge.setProperty(EDGE_RING_ID, ringId);
            }
            if (addedVertexes == batchCount) {
                Edge edge = graph.addEdge(null, vertexes.get(i), vertexes.get(0), EDGE_CLASS);
                edge.setProperty(EDGE_RING_ID, ringId);
            }
        }
        graph.commit();
        LOG.info("Ring " + ringId + " was created while UPDATE by thread " + Thread.currentThread().getId());
        checkRingCreated(graph, ids);
    }

    private void readAllRings(OrientGraph graph, long iteration) {
        Vertex vertex = randomlySelectVertex();
        List<Long> ringIds = vertex.getProperty(RING_IDS);
        //TODO: not implemented yet
    }
}
