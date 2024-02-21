/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.debezium;

import io.airbyte.cdk.components.debezium.DebeziumComponent;
import io.airbyte.cdk.components.debezium.DebeziumComponentIntegrationTest;
import io.airbyte.cdk.components.debezium.RelationalConfigBuilder;
import io.airbyte.cdk.db.PgLsn;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.postgres.PostgresTestDatabase;
import io.debezium.connector.postgresql.PostgresConnector;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class PostgresDebeziumComponentIntegrationTest extends DebeziumComponentIntegrationTest {

  PostgresTestDatabase testdb;

  static final String CREATE_TABLE_KV = """
                                        CREATE TABLE kv (k SERIAL PRIMARY KEY, v VARCHAR(60));
                                        """;

  static final String CREATE_TABLE_EVENTLOG =
      """
      CREATE TABLE eventlog (id UUID GENERATED ALWAYS AS (MD5(entry)::UUID) STORED, entry VARCHAR(60) NOT NULL);
      ALTER TABLE eventlog REPLICA IDENTITY FULL;
      """;

  @Override
  public void applyToSource(@NotNull List<Change> changes) {
    for (var change : changes) {
      var sql = switch (change.kind()) {
        case INSERT -> String.format("INSERT INTO %s (%s) VALUES ('%s');",
            change.table(), change.table().getValueColumnName(), change.newValue());
        case DELETE -> String.format("DELETE FROM %s WHERE %s = '%s';",
            change.table(), change.table().getValueColumnName(), change.oldValue());
        case UPDATE -> String.format("UPDATE %s SET %s = '%s' WHERE %s = '%s';",
            change.table(),
            change.table().getValueColumnName(), change.newValue(),
            change.table().getValueColumnName(), change.oldValue());
      };
      testdb.with(sql);
    }
    testdb.with("CHECKPOINT");

  }

  @Override
  public void bulkInsertSourceKVTable(int numRows) {
    testdb.with("INSERT INTO kv (v) SELECT n::VARCHAR FROM GENERATE_SERIES(1, %d) AS n", numRows)
        .with("CHECKPOINT");
  }

  @NotNull
  @Override
  public DebeziumComponent.State currentSourceState() {
    try {
      final PgLsn lsn = PgLsn.fromPgString(testdb.getDatabase().query(ctx -> ctx
          .selectFrom("pg_current_wal_lsn()")
          .fetchSingle(0, String.class)));
      long txid = testdb.getDatabase().query(ctx -> ctx
          .selectFrom("txid_current()")
          .fetchSingle(0, Long.class));
      var now = Instant.now();
      var value = new HashMap<String, Object>();
      value.put("transaction_id", null);
      value.put("lsn", lsn.asLong());
      value.put("txId", txid);
      value.put("ts_usec", now.toEpochMilli() * 1_000);
      var valueJson = Jsons.jsonNode(value);
      var keyJson = Jsons.arrayNode()
          .add(testdb.getDatabaseName())
          .add(Jsons.jsonNode(Map.of("server", testdb.getDatabaseName())));
      var offset = new DebeziumComponent.State.Offset(Map.of(keyJson, valueJson));
      return new DebeziumComponent.State(offset, Optional.empty());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @NotNull
  @Override
  public DebeziumComponent.Input.Config config() {
    return PostgresConfigBuilder.builder()
        .withDatabaseHost(testdb.getContainer().getHost())
        .withDatabasePort(testdb.getContainer().getFirstMappedPort())
        .withDatabaseUser(testdb.getUserName())
        .withDatabasePassword(testdb.getPassword())
        .withDatabaseName(testdb.getDatabaseName())
        .withCatalog(configuredCatalog())
        .withHeartbeats(Duration.ofMillis(100))
        .with("snapshot.mode", "initial")
        .with("slot.name", testdb.getReplicationSlotName())
        .with("publication.name", testdb.getPublicationName())
        .build();
  }

  @NotNull
  @Override
  public Predicate<DebeziumComponent.Output.Record> generateBoundsPredicate() {
    try {
      final PgLsn lsn = PgLsn.fromPgString(testdb.getDatabase().query(ctx -> ctx
          .selectFrom("pg_current_wal_insert_lsn()")
          .fetchSingle(0, String.class)));
      return r -> isWithinBounds(lsn, r);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  static boolean isWithinBounds(PgLsn upperBound, DebeziumComponent.Output.Record record) {
    PgLsn eventLsn;
    var sourceOffset = record.debeziumSourceRecord().map(SourceRecord::sourceOffset).orElse(Map.of());
    if (sourceOffset.containsKey("lsn")) {
      eventLsn = PgLsn.fromLong((Long) sourceOffset.get("lsn"));
    } else if (record.source().has("lsn")) {
      eventLsn = PgLsn.fromLong(record.source().get("lsn").asLong());
    } else {
      return true;
    }
    return eventLsn.compareTo(upperBound) <= 0;
  }

  @BeforeEach
  void setup() {
    testdb = PostgresTestDatabase.in(PostgresTestDatabase.BaseImage.POSTGRES_16, PostgresTestDatabase.ContainerModifier.CONF)
        .with(CREATE_TABLE_KV)
        .with(CREATE_TABLE_EVENTLOG)
        .withReplicationSlot()
        .withPublicationForAllTables()
        .with("CHECKPOINT");
  }

  @AfterEach
  void tearDown() {
    if (testdb != null) {
      testdb.close();
      testdb = null;
    }
  }

  static class PostgresConfigBuilder extends RelationalConfigBuilder<PostgresConfigBuilder> {

    static PostgresConfigBuilder builder() {
      return new PostgresConfigBuilder()
          .withConnector(PostgresConnector.class)
          .with("plugin.name", "pgoutput")
          .with("publication.autocreate.mode", "disabled")
          .with("flush.lsn.source", "false");
    }

  }

}
