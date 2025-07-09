package org.example;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.AsyncRunner;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.gson.JsonObject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

public class ThroughputRunner {

  private static final int DEFAULT_NUMBER_OF_RUNS = 1000000;
  private static final String DEFAULT_CONCURRENCY = "10";
  private static final int DEFAULT_AVG_FANOUT = 10;

  public static JsonObject create500ByteJsonObject() {
    JsonObject dataObject = new JsonObject();

    dataObject.addProperty("transactionId", "a1b2c3d4-e5f6-7890-1234-567890abcdef");
    dataObject.addProperty("timestamp", "2025-06-23T16:27:24Z");
    dataObject.addProperty("status", "SUCCESS");
    dataObject.addProperty("sourceSystem", "WebApp-Primary");
    dataObject.addProperty("isValidated", true);
    dataObject.addProperty("retryAttempts", 3);
    dataObject.addProperty("processingTimeMs", 125);

    JsonObject userDetails = new JsonObject();
    userDetails.addProperty("userId", "user-98765");
    userDetails.addProperty("username", "jane_doe_extra_long_username_for_padding");
    userDetails.addProperty("emailVerified", true);

    dataObject.add("userDetails", userDetails);

    String description =
        "This is a sample description designed to add more weight to the JSON object, reaching"
            + " closer to the 500-byte target. More text content helps in bloating the size"
            + " efficiently without complex structures.";
    dataObject.addProperty("description", description);

    return dataObject;
  }

  private static ApiFuture<Long> dmlAsyncRunner(
      AsyncRunner runner, String query, Map<String, Value> params, ExecutorService executor) {
    return runner.runAsync(
        txn -> {
          Statement.Builder builder = Statement.newBuilder(query);
          for (Map.Entry<String, Value> param : params.entrySet()) {
            builder.bind(param.getKey()).to(param.getValue());
          }
          return txn.executeUpdateAsync(builder.build());
        },
        executor);
  }

  private static ApiFuture<List<List<Value>>> queryAsyncRunner(
      DatabaseClient client, String query, Map<String, Value> params, ExecutorService executor) {
    Statement.Builder builder = Statement.newBuilder(query);
    for (Map.Entry<String, Value> param : params.entrySet()) {
      builder.bind(param.getKey()).to(param.getValue());
    }
    AsyncResultSet resultSet = client.singleUse().executeQueryAsync(builder.build());
    return resultSet.toListAsync(
        reader -> {
          List<Value> result = new ArrayList<>();
          for (int i = 0; i < reader.getColumnCount(); ++i) {
            result.add(reader.getValue(i));
          }
          return result;
        },
        executor);
  }

  // Blind write node.details = details
  private static ApiFuture<Long> blindUpdateNode(
      AsyncRunner runner, String label, String key, JsonObject details, ExecutorService executor) {

    Map<String, Value> params = new HashMap<>();
    params.put("label", Value.string(label));
    params.put("key", Value.string(key));
    params.put("details", Value.json(details.toString()));
    return dmlAsyncRunner(
        runner,
        "UPDATE GraphNode SET details = @details WHERE label = @label AND key = @key",
        params,
        executor);
  }

  // insert or ignore with return: also a RMW
  private static ApiFuture<Long> findOrCreateNode(
      AsyncRunner runner, String label, String key, JsonObject details, ExecutorService executor) {

    Map<String, Value> params = new HashMap<>();
    params.put("label", Value.string(label));
    params.put("key", Value.string(key));
    params.put("details", Value.json(details.toString()));
    return dmlAsyncRunner(
        runner,
        """
        INSERT OR IGNORE INTO GraphNode (label, key, details) VALUES (@label, @key, @details)
        THEN RETURN details
        """,
        params,
        executor);
  }

  // insert or ignore with return: also a RMW
  private static ApiFuture<Long> findOrCreateEdge(
      AsyncRunner runner,
      String label,
      String key,
      String edgeLabel,
      String otherNodeLabel,
      String otherNodeKey,
      JsonObject details,
      ExecutorService executor) {

    Map<String, Value> params = new HashMap<>();
    params.put("label", Value.string(label));
    params.put("key", Value.string(key));
    params.put("edge_label", Value.string(edgeLabel));
    params.put("other_node_label", Value.string(otherNodeLabel));
    params.put("other_node_key", Value.string(otherNodeKey));
    params.put("details", Value.json(details.toString()));
    return dmlAsyncRunner(
        runner,
        """
        INSERT OR IGNORE INTO GraphEdge
          (label, key, edge_label, other_node_label, other_node_key, details)
        VALUES (@label, @key, @edge_label, @other_node_label, @other_node_key, @details)
        THEN RETURN details
        """,
        params,
        executor);
  }

  // TODO: to be implemented
  // private static ApiFuture<Long> findOrCreateEdgeMutation(
  //     AsyncRunner runner,
  //     String label,
  //     String key,
  //     String edgeLabel,
  //     String otherNodeLabel,
  //     String otherNodeKey,
  //     JsonObject details,
  //     ExecutorService executor) {
  //   return runner.runAsync(
  //       txn -> {
  //         ApiFuture<Struct> existingEdge =
  //             txn.readRowAsync(
  //                 "GraphEdge",
  //                 Key.of(label, key, edgeLabel, otherNodeLabel, otherNodeKey),
  //                 ImmutableList.of("details"));
  //         ApiFuture<Long> result =
  //             ApiFutures.transformAsync(
  //                 existingEdge,
  //                 (edge) -> {
  //                   if (edge != null) {
  //                     return ApiFutures.immediateFuture(0);
  //                   }
  //                   return txn.bufferAsync(
  //                       Mutation.newInsertBuilder("GraphEdge")
  //                           .set("label")
  //                           .to(label)
  //                           .set("key")
  //                           .to(key)
  //                           .set("edge_label")
  //                           .to(edgeLabel)
  //                           .set("other_node_label")
  //                           .to(otherNodeLabel)
  //                           .set("other_node_key")
  //                           .to(otherNodeKey)
  //                           .set("details")
  //                           .to(details)
  //                           .build());
  //                 },
  //                 executor);
  //         return result;
  //       },
  //       executor);
  // }

  private static ApiFuture<Long> updateNodeUpdCount(
      AsyncRunner runner, String label, String key, ExecutorService executor) {
    Map<String, Value> params = new HashMap<>();
    params.put("label", Value.string(label));
    params.put("key", Value.string(key));
    return dmlAsyncRunner(
        runner,
        """
        UPDATE GraphNode AS node
        SET node.details = JSON_SET(node.details, '$.upd_count', COALESCE(INT64(node.details.upd_count), 0) + 1)
        WHERE node.label = @label AND node.key = @key
        """,
        params,
        executor);
  }

  // This is better than:
  //  details = findOrCreateNode(label, key, details)
  //  blindUpdateNode(label, key, details)
  private static ApiFuture<Long> insertOrUpdateNodeUpdCount(
      AsyncRunner runner, String label, String key, JsonObject details, ExecutorService executor) {
    Map<String, Value> params = new HashMap<>();
    params.put("label", Value.string(label));
    params.put("key", Value.string(key));
    params.put("details", Value.json(details.toString()));
    return dmlAsyncRunner(
        runner,
        """
        INSERT OR UPDATE INTO GraphNode
          (label, key, details)
        VALUES (@label, @key, COALESCE(
            (
                SELECT JSON_SET(details, '$.upd_count', COALESCE(INT64(details.upd_count), 0) + 1)
                FROM GraphNode
                WHERE label = @label AND key = @key
            )
            , @details))
        """,
        params,
        executor);
  }

  // NOTE: must ensure the node endpoints already exists.
  private static ApiFuture<Long> insertOrUpdateEdgeUpdCount(
      AsyncRunner runner,
      String label,
      String key,
      String edgeLabel,
      String otherNodeLabel,
      String otherNodeKey,
      JsonObject details,
      ExecutorService executor) {
    Map<String, Value> params = new HashMap<>();
    params.put("label", Value.string(label));
    params.put("key", Value.string(key));
    params.put("edge_label", Value.string(edgeLabel));
    params.put("other_node_label", Value.string(otherNodeLabel));
    params.put("other_node_key", Value.string(otherNodeKey));
    params.put("details", Value.json(details.toString()));
    return dmlAsyncRunner(
        runner,
        """
        INSERT OR UPDATE INTO GraphEdge
          (label, key, edge_label, other_node_label, other_node_key, details)
        VALUES (@label, @key, @edge_label, @other_node_label, @other_node_key, COALESCE(
            (
                SELECT JSON_SET(details, '$.upd_count', COALESCE(INT64(details.upd_count), 0) + 1)
                FROM GraphEdge
                WHERE label = @label AND key = @key AND edge_label = @edge_label
                  AND other_node_label = @other_node_label AND other_node_key = @other_node_key
            )
            , @details))
        """,
        params,
        executor);
  }

  private static ApiFuture<List<List<Value>>> findSubgraph(
      DatabaseClient client, String label, String key, ExecutorService executor) {
    Map<String, Value> params = new HashMap<>();
    params.put("label", Value.string(label));
    params.put("key", Value.string(key));
    return queryAsyncRunner(
        client,
        """
        GRAPH AFGraphSchemaless
        MATCH (n {label:@label, key:@key}) -[e:Forward|Reverse]-> (m)
        RETURN n.details AS n_details, e.edge_label, e.details,
               m.label AS m_label, m.key AS m_key, m.details AS m_details
        """,
        params,
        executor);
  }

  private static AsyncRunner getRunner(
      DatabaseClient dbClient, String experiment, int maxCommitDelayMillis) {
    if (maxCommitDelayMillis < 0) {
      return dbClient.runAsync(
          com.google.cloud.spanner.Options.tag("app=wmt-graph-poc,service=" + experiment));
    }
    return dbClient.runAsync(
        com.google.cloud.spanner.Options.maxCommitDelay(Duration.ofMillis(maxCommitDelayMillis)),
        com.google.cloud.spanner.Options.tag("app=wmt-graph-poc,service=" + experiment));
  }

  private static void handleDmlResult(
      ApiFuture<Long> future,
      int numOperations,
      Semaphore semaphore,
      AtomicInteger completedCount,
      AtomicInteger failureCount,
      AtomicInteger numRows,
      ExecutorService workerExecutor) {
    ApiFutures.addCallback(
        future,
        new ApiFutureCallback<Long>() {
          @Override
          public void onSuccess(Long result) {
            int currentCompleted = completedCount.incrementAndGet();
            if (currentCompleted % 10000 == 0) {
              System.out.printf(
                  "Completed %,d of %,d operations...%n", currentCompleted, numOperations);
            }
            numRows.addAndGet(result.intValue());
            semaphore.release();
          }

          @Override
          public void onFailure(Throwable ex) {
            int currentFailure = failureCount.incrementAndGet();
            if (currentFailure % 1000 == 1) {
              System.out.printf("Exception: " + ex.getMessage());
            }
            onSuccess(0L);
          }
        },
        workerExecutor);
  }

  private static void handleQueryResult(
      ApiFuture<List<List<Value>>> future,
      int numOperations,
      Semaphore semaphore,
      AtomicInteger completedCount,
      AtomicInteger failureCount,
      AtomicInteger numRows,
      ExecutorService workerExecutor) {
    ApiFutures.addCallback(
        future,
        new ApiFutureCallback<List<List<Value>>>() {
          @Override
          public void onSuccess(List<List<Value>> result) {
            int currentCompleted = completedCount.incrementAndGet();
            if (currentCompleted % 10000 == 0) {
              System.out.printf(
                  "Completed %,d of %,d operations...%n", currentCompleted, numOperations);
            }
            numRows.addAndGet(result.size());
            semaphore.release();
          }

          @Override
          public void onFailure(Throwable ex) {
            int currentFailure = failureCount.incrementAndGet();
            if (currentFailure % 1000 == 1) {
              System.out.printf("Exception: " + ex.getMessage());
            }
            List<List<Value>> emptyList = new ArrayList<>();
            onSuccess(emptyList);
          }
        },
        workerExecutor);
  }

  // Remember to run ANALYZE after graph generation to keep statistics up-to-date.
  private static void generateGraph(
      DatabaseClient dbClient,
      int numLabels,
      int numKeys,
      int averageFanout,
      int concurrency,
      ExecutorService executor)
      throws java.lang.InterruptedException {
    Semaphore semaphore = new Semaphore(concurrency);
    AtomicInteger completedCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);
    AtomicInteger numRows = new AtomicInteger(0);
    JsonObject hardcodedDetails = create500ByteJsonObject();
    int numNodes = numLabels * numKeys;
    int numEdges = numNodes * averageFanout;
    int total = numNodes + numEdges;
    System.out.printf(
        "Generate graph with %d nodes and %d edges...\n",
        numLabels * numKeys, numLabels * numKeys * averageFanout);

    int numPartitions = 100;
    int numOpPerPartition = (numNodes + numPartitions - 1) / numPartitions;
    for (int p = 0; p < numOpPerPartition; ++p) {
      for (int q = 0; q < numPartitions; ++q) {
        semaphore.acquire();
        int offset = q * numOpPerPartition + p;
        int i = offset / numKeys;
        int j = offset % numKeys;
        AsyncRunner runner = getRunner(dbClient, "generateGraph", -1);
        ApiFuture<Long> future =
            insertOrUpdateNodeUpdCount(
                runner, String.valueOf(i), String.valueOf(j), hardcodedDetails, executor);
        handleDmlResult(future, total, semaphore, completedCount, failureCount, numRows, executor);
      }
    }
    for (int p = 0; p < numOpPerPartition; ++p) {
      for (int q = 0; q < numPartitions; ++q) {
        int offset = q * numOpPerPartition + p;
        int i = offset / numKeys;
        int j = offset % numKeys;
        for (int k = 0; k < averageFanout; ++k) {
          semaphore.acquire();
          String edgeLabel = String.valueOf(Math.abs(UUID.randomUUID().hashCode()) % numLabels);
          String otherNodeLabel =
              String.valueOf(Math.abs(UUID.randomUUID().hashCode()) % numLabels);
          String otherNodeKey = String.valueOf(Math.abs(UUID.randomUUID().hashCode()) % numKeys);
          AsyncRunner runner = getRunner(dbClient, "generateGraph", -1);
          ApiFuture<Long> future =
              insertOrUpdateEdgeUpdCount(
                  runner,
                  String.valueOf(i),
                  String.valueOf(j),
                  edgeLabel,
                  otherNodeLabel,
                  otherNodeKey,
                  hardcodedDetails,
                  executor);
          handleDmlResult(
              future, total, semaphore, completedCount, failureCount, numRows, executor);
        }
      }
    }

    System.out.println("All requests submitted. Waiting for the final operations to complete...");
    while (completedCount.get() < total) {
      Thread.sleep(1);
    }
  }

  public static void main(String[] args) throws ParseException {
    org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
    options.addOption("p", "project", true, "project name");
    options.addOption("i", "instance", true, "instance name");
    options.addOption("d", "database", true, "database name");
    options.addOption(
        "e",
        "experiment",
        true,
        "Experiment mode: blindUpdateNode, insertOrUpdateNodeUpdCount, findOrCreateNode,"
            + " insertOrUpdateEdgeUpdCount, findSubgraph, findOrCreateEdge");
    options.addOption("n", "numKeys", true, "Total number of deterministic keys in the set");
    options.addOption("nr", "numOperations", true, "Total number of operations");
    options.addOption("l", "numLabels", true, "Total number of deterministic labels in the set");
    options.addOption("c", "maxCommitDelay", true, "Max commit delay in milliseconds");
    options.addOption("cc", "concurrency", true, "Concurrency");

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    String project = commandLine.getOptionValue("project");
    String instance = commandLine.getOptionValue("instance");
    String database = commandLine.getOptionValue("database");
    String experiment = commandLine.getOptionValue("experiment");
    int numOperations =
        Integer.parseInt(
            commandLine.getOptionValue("numOperations", String.valueOf(DEFAULT_NUMBER_OF_RUNS)));
    int numKeys =
        Integer.parseInt(commandLine.getOptionValue("numKeys", String.valueOf(numOperations)));
    int numLabels = Integer.parseInt(commandLine.getOptionValue("numLabels", String.valueOf(1)));
    int maxCommitDelayMillis = Integer.parseInt(commandLine.getOptionValue("maxCommitDelay", "-1"));
    int concurrency =
        Integer.parseInt(commandLine.getOptionValue("concurrency", DEFAULT_CONCURRENCY));

    Semaphore semaphore = new Semaphore(concurrency);
    AtomicInteger completedCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);
    AtomicInteger numRows = new AtomicInteger(0);

    final int numChannels = Runtime.getRuntime().availableProcessors();
    System.out.printf("Optimizing Spanner client with %d gRPC channels.%n", numChannels);
    System.out.printf("Project: %s, Instance: %s, Database: %s\n", project, instance, database);

    SpannerOptions spannerOptions =
        SpannerOptions.newBuilder().setProjectId(project).setNumChannels(numChannels).build();

    Spanner spanner = spannerOptions.getService();
    ExecutorService workerExecutor = Executors.newFixedThreadPool(concurrency);

    try {
      DatabaseClient dbClient =
          spanner.getDatabaseClient(
              DatabaseId.of(spannerOptions.getProjectId(), instance, database));
      if (experiment.equals("generateGraph")) {
        generateGraph(
            dbClient, numLabels, numKeys, DEFAULT_AVG_FANOUT, concurrency, workerExecutor);
        return;
      }

      System.out.printf(
          "Starting throughput test in '%s' mode with %,d operations...%n",
          experiment, numOperations);

      JsonObject hardcodedDetails = create500ByteJsonObject();
      long startTime = System.currentTimeMillis();
      for (int i = 0; i < numOperations; i++) {
        semaphore.acquire();
        String key = String.valueOf(Math.abs(UUID.randomUUID().hashCode()) % numKeys);
        String label = String.valueOf(Math.abs(UUID.randomUUID().hashCode()) % numLabels);

        if (experiment.equals("findSubgraph")) {
          ApiFuture<List<List<Value>>> future = findSubgraph(dbClient, label, key, workerExecutor);
          handleQueryResult(
              future,
              numOperations,
              semaphore,
              completedCount,
              failureCount,
              numRows,
              workerExecutor);
          continue;
        }

        AsyncRunner runner = getRunner(dbClient, experiment, maxCommitDelayMillis);

        ApiFuture<Long> future;

        if (experiment.equals("insertOrUpdateNodeUpdCount")) {
          future = insertOrUpdateNodeUpdCount(runner, label, key, hardcodedDetails, workerExecutor);
        } else if (experiment.equals("findOrCreateNode")) {
          future = findOrCreateNode(runner, label, key, hardcodedDetails, workerExecutor);
        } else if (experiment.equals("findOrCreateEdge")) {
          String edgeLabel = String.valueOf(Math.abs(UUID.randomUUID().hashCode()) % numLabels);
          String otherNodeLabel =
              String.valueOf(Math.abs(UUID.randomUUID().hashCode()) % numLabels);
          String otherNodeKey = String.valueOf(Math.abs(UUID.randomUUID().hashCode()) % numKeys);
          future =
              findOrCreateEdge(
                  runner,
                  label,
                  key,
                  edgeLabel,
                  otherNodeLabel,
                  otherNodeKey,
                  hardcodedDetails,
                  workerExecutor);
        } else if (experiment.equals("blindUpdateNode")) {
          future = blindUpdateNode(runner, label, key, hardcodedDetails, workerExecutor);
        } else if (experiment.equals("insertOrUpdateEdgeUpdCount")) {
          String edgeLabel = String.valueOf(Math.abs(UUID.randomUUID().hashCode()) % numLabels);
          String otherNodeLabel =
              String.valueOf(Math.abs(UUID.randomUUID().hashCode()) % numLabels);
          String otherNodeKey = String.valueOf(Math.abs(UUID.randomUUID().hashCode()) % numKeys);
          future =
              insertOrUpdateEdgeUpdCount(
                  runner,
                  label,
                  key,
                  edgeLabel,
                  otherNodeLabel,
                  otherNodeKey,
                  hardcodedDetails,
                  workerExecutor);
        } else {
          throw new RuntimeException("Invalid experiment: " + experiment);
        }
        handleDmlResult(
            future,
            numOperations,
            semaphore,
            completedCount,
            failureCount,
            numRows,
            workerExecutor);
      }

      System.out.println("All requests submitted. Waiting for the final operations to complete...");
      while (completedCount.get() < numOperations) {
        Thread.sleep(1);
      }

      long endTime = System.currentTimeMillis();
      double durationInSeconds = (endTime - startTime) / 1000.0;
      double throughput = numOperations / durationInSeconds;

      System.out.println("-------------------------------------------------");
      System.out.println("Test Complete");
      System.out.printf("Mode: %s%n", experiment);
      System.out.printf("Total Operations: %,d%n", numOperations);
      System.out.printf("Successful: %,d%n", numOperations - failureCount.get());
      System.out.printf("Failed: %,d%n", failureCount.get());
      System.out.printf("NumRows: %,d%n", numRows.get());
      System.out.printf("Total Time: %.2f seconds%n", durationInSeconds);
      System.out.printf("Throughput: %.2f operations/second%n", throughput);
      System.out.printf(
          "MaxCommitDelayMillis: %d, WorkerThreadPoolSize: %d\n",
          maxCommitDelayMillis, concurrency);
      System.out.println("-------------------------------------------------");

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      spanner.close();
      workerExecutor.shutdown();
    }
  }
}
