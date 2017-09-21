import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
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

import java.util.*;
import java.util.concurrent.*;

public class TransactionsTest extends CreateGraphDatabaseFixture {

    private static final String VERTEX_CLASS = "TestVertexClass";
    private static final String VERTEX_ID = "vertexId";
    private static final String CREATOR_ID = "creatorId";
    private static final String BATCH_COUNT = "batchCount";
    private static final String ITERATION = "iteration";
    private static final String RND = "rnd";
    private static final String RING_ID = "ringId";
    private static final String EDGE_LABEL = "connects";

    public static final Logger LOG = LoggerFactory.getLogger(TransactionsTest.class);

    @Test
    public void shouldCheckTransactions() throws InterruptedException, ExecutionException {
        OrientGraphNoTx graphNoTx = factory.getNoTx();
        OClass clazz = graphNoTx.createVertexType(VERTEX_CLASS);
        graphNoTx.createEdgeType(EDGE_LABEL);
        createProperties(clazz);
        createIndexes(clazz);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Callable<Object>> tasksToCreate = new ArrayList<>();
        List<Callable<Object>> tasksToDelete = new ArrayList<>();

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
            for (int i = 0; i < 4; i++) {
                tasksToDelete.add(() -> {
                    OrientGraph graph;
                    try {
                        graph = factory.getTx();
                        while (true) {
                            if (Counter.getDeleted() < BasicUtils.getDeletedLimit()
                                    && Counter.getVertexesNumber() > BasicUtils.getMaxBatch()) {
                                deleteVertexesAndEdges(graph);
                            } else if (Counter.getDeleted() >= BasicUtils.getDeletedLimit()) {
                                break;
                            }
                        }
                        LOG.info("Graph shutdown by deleting thread " + Thread.currentThread().getId());
                        graph.shutdown();
                        return null;
                    } catch (Exception e) {
                        LOG.error("Exception during operation processing", e);
                        throw e;
                    }
                });
            }

            tasksToCreate.addAll(tasksToDelete);

            List<Future<Object>> futures = executor.invokeAll(tasksToCreate);
            for (Future future : futures) {
                future.get();
            }
        } finally {
            executor.shutdown();
        }
    }

    private void createProperties(OClass clazz) {
        clazz.createProperty(VERTEX_ID, OType.LONG);
        clazz.createProperty(CREATOR_ID, OType.LONG);
        clazz.createProperty(BATCH_COUNT, OType.INTEGER);
        clazz.createProperty(ITERATION, OType.LONG);
        clazz.createProperty(RND, OType.LONG);
        clazz.createProperty(RING_ID, OType.LONG);
    }

    private void createIndexes(OClass clazz) {
        clazz.createIndex(VERTEX_CLASS + "." + VERTEX_ID, OClass.INDEX_TYPE.NOTUNIQUE, VERTEX_ID);
        clazz.createIndex(VERTEX_CLASS + "." + RND, OClass.INDEX_TYPE.UNIQUE, RND);
        clazz.createIndex(VERTEX_CLASS + "." + ITERATION + "_" + CREATOR_ID + "_" + VERTEX_ID,
                OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, ITERATION, CREATOR_ID, VERTEX_ID);
    }

    private void addVertexesAndEdges(OrientGraph graph, long iterationNumber) {
        int batchCount = BasicUtils.generateBatchSize();
        List<Vertex> vertexes = new ArrayList<>(batchCount);
        List<Long> ids = new ArrayList<>();
        long ringId = Counter.getNextRingId();

        long threadId = Thread.currentThread().getId();
        try {
            for (int i = 0; i < batchCount; i++) {
                Vertex vertex = graph.addVertex("class:" + VERTEX_CLASS);
                long vertexId = Counter.getNextVertexId();
                ids.add(vertexId);
                vertex.setProperty(VERTEX_ID, vertexId);
                vertex.setProperty(CREATOR_ID, threadId);
                vertex.setProperty(BATCH_COUNT, batchCount);
                vertex.setProperty(ITERATION, iterationNumber);
                vertex.setProperty(RND, BasicUtils.generateRnd());

                vertex.setProperty(RING_ID, ringId);

                vertexes.add(vertex);
                int addedVertexes = vertexes.size();

                if (addedVertexes > 1) {
                    graph.addEdge(null, vertexes.get(i - 1), vertexes.get(i), EDGE_LABEL);
                }
                if (addedVertexes == batchCount) {
                    graph.addEdge(null, vertexes.get(i), vertexes.get(0), EDGE_LABEL);
                }
            }
            BasicUtils.keepRing(ringId);
            graph.commit();
            LOG.info("Ring " + ringId + " was created by thread " + threadId);

            //actions after commit
            performSelectOperations(graph, ids, iterationNumber, threadId, ids.size(), 1);
            checkRingCreated(graph, ids);
            checkClusterPositionsPositive(vertexes);
            BasicUtils.allowDeleteRing(ringId);
        } catch (ORecordDuplicatedException e) {
            LOG.error("Duplicated record", e);
            graph.rollback();
            BasicUtils.allowDeleteRing(ringId);
            //actions after rollback
            performSelectOperations(graph, ids, iterationNumber, threadId, 0, 0);
        } catch (Exception e) {
            LOG.error("Exception was caught");
            graph.rollback();
            BasicUtils.allowDeleteRing(ringId);
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
                "select * from V where " + VERTEX_ID + " <= " + lastId + " and " + ITERATION + " = " + iteration
                        + " and " + CREATOR_ID + " = " + threadId + " order by " + VERTEX_ID + " limit " + limit))
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
        List<Integer> creatorIds = new ArrayList<>();
        List<Long> iterationNumbers = new ArrayList<>();

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

        int batchCount = vertex.getProperty(BATCH_COUNT);

        for (int i = 0; i < batchCount; i++) {
            creatorIds.add(vertex.getProperty(CREATOR_ID));
            iterationNumbers.add(vertex.getProperty(ITERATION));
            Iterable<Edge> edges = vertex.getEdges(Direction.OUT, EDGE_LABEL);
            Assert.assertTrue(edges.iterator().hasNext(),
                    "Edge OUT doesn't exist in vertex " + vertex.getProperty(VERTEX_ID));
            Vertex nextVertex = edges.iterator().next().getVertex(Direction.IN);

            long vertexId;
            if (i == batchCount - 1) {
                vertexId = ids.get(0);
            } else {
                vertexId = ids.get(i + 1);
            }

            boolean connected = nextVertex.getProperty(VERTEX_ID).equals(vertexId);
            Assert.assertTrue(connected, "Vertexes are not correctly connected by edges");
            vertex = nextVertex;

        }
        boolean isOneThread = creatorIds.stream().distinct().limit(2).count() <= 1;
        boolean isOneIteration = iterationNumbers.stream().distinct().limit(2).count() <= 1;
        Assert.assertTrue(isOneThread, "Vertexes are not created by one thread");
        Assert.assertTrue(isOneIteration, "Vertexes are not created during one iteration");
    }

    private void checkClusterPositionsPositive(List<Vertex> vertexes) {
        for (int i = 0; i < vertexes.size(); i++) {
            long clusterPosition = ((OrientVertex) vertexes.get(i)).getIdentity().getClusterPosition();
            Assert.assertTrue(clusterPosition >= 0, "Cluster position in a record is not positive");
        }
    }

    private void deleteVertexesAndEdges(OrientGraph graph) {
        boolean success = false;
        long firstVertex;
        long ringId = -1;
        Vertex vertex = null;
        OrientDynaElementIterable firstVertexResult;
        while (!success) {
            firstVertex = BasicUtils.getRandomVertexId();
            firstVertexResult = graph
                    .command(new OCommandSQL("select from V where " + VERTEX_ID + " = " + firstVertex))
                    .execute();
            if (firstVertexResult.iterator().hasNext()) {
                vertex = (OrientVertex) firstVertexResult.iterator().next();
                ringId = vertex.getProperty(RING_ID);
                if (!BasicUtils.containsAndSetRingId(ringId)) {
                    success = true;
                }
            }
        }

        try {
            LOG.info("Vertex from ring " + ringId + " is chosen by thread " + Thread.currentThread().getId());

            int batchCount = vertex.getProperty(BATCH_COUNT);

            List<Vertex> vertexes = new ArrayList<>();

            for (int i = 0; i < batchCount; i++) {
                Iterable<Edge> edges = vertex.getEdges(Direction.OUT, EDGE_LABEL);
                Assert.assertTrue(edges.iterator().hasNext(),
                        "Edge OUT doesn't exist in vertex " + vertex.getProperty(VERTEX_ID));
                Vertex nextVertex = edges.iterator().next().getVertex(Direction.IN);
                vertexes.add(nextVertex);
                vertex = nextVertex;
            }

            List<Long> deletedIds = new ArrayList<>(vertexes.size());
            for (Vertex v : vertexes) {
                long idToDelete = v.getProperty(VERTEX_ID);
                v.remove();
                deletedIds.add(idToDelete);
            }
            Collections.sort(deletedIds);

            try {
                graph.commit();
                BasicUtils.allowDeleteRing(ringId);
                for (int i = 0; i < vertexes.size(); i++) {
                    Counter.incrementDeleted();
                }
            } catch (ORecordNotFoundException e) {
                LOG.error("Vertex from " + ringId + " ring is not found by thread " + Thread.currentThread().getId());
            }
            LOG.info("Ring " + ringId + " was deleted by thread " + Thread.currentThread().getId());


            selectByIds(graph, deletedIds, 0);
        } catch (NullPointerException e) {
            LOG.error("Vertex NullPointerException in thread " + Thread.currentThread().getId());
        }
    }
}