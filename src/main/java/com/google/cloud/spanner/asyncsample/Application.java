/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.asyncsample;

import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.AsyncTransactionManager;
import com.google.cloud.spanner.AsyncTransactionManager.CommitTimestampFuture;
import com.google.cloud.spanner.AsyncTransactionManager.TransactionContextFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ManagedChannelBuilder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class Application {
  static GenericContainer<?> emulator;

  public static void main(String[] args) throws Exception {
    startEmulator();
    Spanner spanner =
        SpannerOptions.newBuilder()
            .setProjectId("emulator-project")
            .setEmulatorHost(emulator.getHost() + ":" + emulator.getMappedPort(9010))
            .setCredentials(NoCredentials.getInstance())
            .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
            .build()
            .getService();
    DatabaseId databaseId = DatabaseId.of("emulator-project", "test-instance", "test-database");
    EmulatorUtil.maybeCreateInstanceAndDatabase(spanner, databaseId, Dialect.GOOGLE_STANDARD_SQL);

    DatabaseAdminClient adminClient = spanner.getDatabaseAdminClient();
    adminClient.updateDatabaseDdl(
        databaseId.getInstanceId().getInstance(),
        databaseId.getDatabase(),
        ImmutableList.of("create table test (id int64, value string(max)) primary key (id)"),
        null);
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    try {
      for (int i = 0; i < 2; i++) {
        // The second time around, this transaction will fail, as it tries to insert a row with the
        // same ID.
        runAsyncTransaction(client, i);
      }
    } finally {
      spanner.close();
      emulator.stop();
    }
  }

  static void runAsyncTransaction(DatabaseClient client, int i) throws Exception {
    try (AsyncTransactionManager manager = client.transactionManagerAsync()) {
      TransactionContextFuture transactionFuture = manager.beginAsync();
      while (true) {
        CommitTimestampFuture commitTimestamp =
            transactionFuture
                .then(
                    (transaction, row) ->
                        transaction.bufferAsync(
                            Mutation.newUpdateBuilder("test")
                                .set("id")
                                .to(1L)
                                .set("value")
                                .to("One")
                                .build()),
                    MoreExecutors.directExecutor())
                .commitAsync();
        try {
          Timestamp timestamp = commitTimestamp.get();
          System.out.printf("Transaction %d succeeded and committed at %s\n", i, timestamp);
          break;
        } catch (AbortedException e) {
          Thread.sleep(e.getRetryDelayInMillis());
          transactionFuture = manager.resetForRetryAsync();
        } catch (Exception exception) {
          // This will fail with the error
          // 'rollback can only be called if the transaction is in progress'
          // if the exception happened during a Commit.
          manager.rollbackAsync().get();
        }
      }
    }
  }

  static void startEmulator() {
    emulator =
        new GenericContainer<>(
                DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator:latest"))
            .withExposedPorts(9010)
            .waitingFor(Wait.forListeningPorts(9010));
    emulator.start();
  }
}
