package io.airbyte.cdk.components.debezium

import io.airbyte.cdk.components.debezium.DebeziumComponent.Output.CompletionReason
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.Field
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.function.Consumer
import java.util.function.Predicate

abstract class DebeziumComponentIntegrationTest {

    fun catalog(): AirbyteCatalog = AirbyteCatalog().withStreams(listOf(
            CatalogHelpers.createAirbyteStream(
                    "kv",
                    "public",
                    Field.of("k", JsonSchemaType.INTEGER),
                    Field.of("v", JsonSchemaType.STRING))
                    .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
                    .withSourceDefinedPrimaryKey(listOf(listOf("v"))),
            CatalogHelpers.createAirbyteStream(
                    "eventlog",
                    "public",
                    Field.of("id", JsonSchemaType.STRING),
                    Field.of("entry", JsonSchemaType.STRING))
                    .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
                    .withSourceDefinedPrimaryKey(listOf(listOf("id")))))


    fun configuredCatalog(): ConfiguredAirbyteCatalog {
        val configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(catalog())
        configuredCatalog.streams.forEach{ it.syncMode = SyncMode.INCREMENTAL }
        return configuredCatalog
    }
    
    abstract fun applyToSource(changes: List<Change>)
    
    abstract fun bulkInsertSourceKVTable(numRows: Int)

    abstract fun currentSourceState(): DebeziumComponent.State
    
    abstract fun config(): DebeziumComponent.Input.Config

    abstract fun generateBoundsPredicate(): Predicate<DebeziumComponent.Output.Record>
    
    fun completionTargets(maxRecords: Long) = DebeziumComponent.Input.CompletionTargets(
            generateBoundsPredicate(),
            maxRecords,
            Duration.ofSeconds(5),
            Duration.ofSeconds(1),
            Duration.ofSeconds(1),
            0.0)


    @JvmRecord
    data class Change(val table: Table, val oldValue: Value?, val newValue: Value?) {
        enum class Table(val valueColumnName: String) {
            KV("v"),
            EVENTLOG("entry")
        }

        enum class Value {
            FOO,
            BAR,
            BAZ,
            QUUX,
            XYZZY
        }

        enum class Kind {
            INSERT,
            UPDATE,
            DELETE
        }

        fun kind(): Kind {
            if (oldValue == null) {
                return Kind.INSERT
            }
            if (newValue == null) {
                return Kind.DELETE
            }
            return Kind.UPDATE
        }
    }
    
    fun insert(table: Change.Table, newValue: Change.Value): Change {
        return Change(table, null, newValue)
    }

    fun delete(table: Change.Table, oldValue: Change.Value): Change {
        return Change(table, oldValue, null)
    }

    fun update(table: Change.Table, oldValue: Change.Value, newValue: Change.Value): Change {
        return Change(table, oldValue, newValue)
    }
    
    val initialInsert: List<Change> = listOf(
            insert(Change.Table.KV, Change.Value.FOO),
            insert(Change.Table.KV, Change.Value.BAR),
            insert(Change.Table.EVENTLOG, Change.Value.FOO),
            insert(Change.Table.EVENTLOG, Change.Value.BAR))

    val updateKV: List<Change> = listOf(
            update(Change.Table.KV, Change.Value.FOO, Change.Value.QUUX),
            update(Change.Table.KV, Change.Value.BAR, Change.Value.XYZZY))

    val deleteEventLog: List<Change> = listOf(
            delete(Change.Table.EVENTLOG, Change.Value.FOO),
            delete(Change.Table.EVENTLOG, Change.Value.BAR))

    val subsequentInsert: List<Change> = listOf(
            insert(Change.Table.KV, Change.Value.BAZ),
            insert(Change.Table.EVENTLOG, Change.Value.BAZ))


    @Test
    fun testNoProgress() {
        applyToSource(initialInsert)
        val input1 = DebeziumComponent.Input(config(), currentSourceState(), completionTargets(1))
        val output1 = DebeziumEngineManager.debeziumComponent().collect(input1)
        assertNoProgress(input1, output1)
        // An annoying characteristic of the debezium engine makes it such that the
        // HAS_WAITED_LONG_ENOUGH_FOR_INITIAL_RECORD reason is never triggered in cases
        // where the (logical) database WAL is consumed starting from the last offset
        // and the WAL is not making any logical progress.
        assertCompletionReason(output1, DebeziumComponent.Output.CompletionReason.HAS_COLLECTED_LONG_ENOUGH)
    }

    @Test
    fun testHeartbeatsProgress() {
        val state0: DebeziumComponent.State = currentSourceState()
        // Insert just one record.
        // We need this or Debezium will never actually start firing heartbeats.
        applyToSource(initialInsert.take(1))
        // Consume the WAL from right before the insert to right after it.
        val input1 = DebeziumComponent.Input(config(), state0, completionTargets(10))
        // Make sure there's more entries in the WAL after the insert.
        bulkInsertSourceKVTable(10_000)
        val output1 = DebeziumEngineManager.debeziumComponent().collect(input1)
        assertCompletionReason(output1, DebeziumComponent.Output.CompletionReason.HAS_EVENTS_OUT_OF_BOUNDS)
    }

    @Test
    @Disabled
    fun testCRUD() {
        val maxRecords = 20
        val state0: DebeziumComponent.State = currentSourceState()
        
        applyToSource(initialInsert)
        val input1 = DebeziumComponent.Input(config(), state0, completionTargets(maxRecords.toLong()))
        val output1 = DebeziumEngineManager.debeziumComponent().collect(input1)
        assertProgress(input1, output1)
        assertData(output1, initialInsert)
        assertCompletionReason(output1, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD)

        applyToSource(deleteEventLog)
        val input2 = DebeziumComponent.Input(config(), output1.state, completionTargets(maxRecords.toLong()))
        val output2 = DebeziumEngineManager.debeziumComponent().collect(input2)
        assertProgress(input2, output2)
        assertData(output2, deleteEventLog)
        assertCompletionReason(output2, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD)

        applyToSource(updateKV)
        val input3 = DebeziumComponent.Input(config(), output2.state, completionTargets(maxRecords.toLong()))
        val output3 = DebeziumEngineManager.debeziumComponent().collect(input3)
        assertProgress(input3, output3)
        assertData(output3, updateKV)
        assertCompletionReason(output3, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD)

        applyToSource(subsequentInsert)
        val input4 = DebeziumComponent.Input(config(), output3.state, completionTargets(maxRecords.toLong()))
        val output4 = DebeziumEngineManager.debeziumComponent().collect(input4)
        assertProgress(input4, output4)
        assertData(output4, subsequentInsert)
        assertCompletionReason(output4, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD)

        val input24 = DebeziumComponent.Input(config(), output1.state, input4.completionTargets)
        val output24 = DebeziumEngineManager.debeziumComponent().collect(input24)
        assertProgress(input24, output24)
        assertData(output24, deleteEventLog, updateKV, subsequentInsert)
        assertCompletionReason(output24, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD)

        val input14 = DebeziumComponent.Input(config(), state0, input4.completionTargets)
        val output14 = DebeziumEngineManager.debeziumComponent().collect(input14)
        assertData(output14, initialInsert, deleteEventLog, updateKV, subsequentInsert)
        assertCompletionReason(output14, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD)
    }

    @Test
    fun testCompletesWithEnoughRecords() {
        val numRows = 10_000
        val maxRecords = 10
        val state0: DebeziumComponent.State = currentSourceState()

        bulkInsertSourceKVTable(numRows)
        val input1 = DebeziumComponent.Input(config(), state0, completionTargets(maxRecords.toLong()))
        val output1 = DebeziumEngineManager.debeziumComponent().collect(input1)
        assertProgress(input1, output1)
        assertCompletionReason(output1, DebeziumComponent.Output.CompletionReason.HAS_COLLECTED_ENOUGH_RECORDS)
        // We actually get more than we bargained for, but that's OK.
        assertDataCountWithinBounds(maxRecords + 1, output1, numRows)

        val input2 = DebeziumComponent.Input(config(), state0, completionTargets(numRows.toLong()))
        val output2 = DebeziumEngineManager.debeziumComponent().collect(input2)
        assertProgress(input2, output2)
        assertCompletionReason(output2, DebeziumComponent.Output.CompletionReason.HAS_COLLECTED_ENOUGH_RECORDS)
        Assertions.assertEquals(numRows, output2.data.size)
    }

    @Test
    fun testCompletesWhenOutOfBounds() {
        val numRowsInBatch = 10_000
        val maxRecords = 100_000
        val state0: DebeziumComponent.State = currentSourceState()
        
        bulkInsertSourceKVTable(numRowsInBatch)
        val completionTargets1 = completionTargets(maxRecords.toLong())
        bulkInsertSourceKVTable(numRowsInBatch)
        val input1 = DebeziumComponent.Input(config(), state0, completionTargets1)
        val output1 = DebeziumEngineManager.debeziumComponent().collect(input1)
        assertProgress(input1, output1)
        assertCompletionReason(output1, DebeziumComponent.Output.CompletionReason.HAS_EVENTS_OUT_OF_BOUNDS)
        assertDataCountWithinBounds(numRowsInBatch, output1, 2 * numRowsInBatch)
    }


    fun assertNoProgress(input: DebeziumComponent.Input, output: DebeziumComponent.Output) {
        Assertions.assertTrue(input.state.schema.isEmpty)
        Assertions.assertTrue(output.state.schema.isEmpty)
        Assertions.assertTrue(output.data.isEmpty())
        Assertions.assertEquals(
                Jsons.serialize(Jsons.jsonNode(input.state.offset.debeziumOffset)),
                Jsons.serialize(Jsons.jsonNode(output.state.offset.debeziumOffset)))
        Assertions.assertEquals(0, output.executionSummary.records.count())
        Assertions.assertNotEquals(setOf<Any>(), output.completionReasons)
    }

    fun assertProgress(input: DebeziumComponent.Input, output: DebeziumComponent.Output) {
        Assertions.assertTrue(input.state.schema.isEmpty)
        Assertions.assertTrue(output.state.schema.isEmpty)
        Assertions.assertNotEquals(
                Jsons.serialize(Jsons.jsonNode(input.state.offset.debeziumOffset)),
                Jsons.serialize(Jsons.jsonNode(output.state.offset.debeziumOffset)))
        Assertions.assertNotEquals(0, output.executionSummary.events.count())
        Assertions.assertNotEquals(0, output.executionSummary.records.count())
        Assertions.assertNotEquals(setOf<Any>(), output.completionReasons)
    }

    @SafeVarargs
    fun assertData(output: DebeziumComponent.Output, vararg expected: List<Change>) {
        val expectedAsInsertsOrDeletes = expected.flatMap { it }
                .map { if (it.kind() == Change.Kind.UPDATE) insert(it.table, it.newValue!!) else it }
        val actualAsInsertsOrDeletes: List<Change> = output.data.map { r ->
            val table = Change.Table.valueOf(r.source()["table"].asText().uppercase())
            val before = r.before()[table.valueColumnName]?.asText()?.uppercase()?.let {  Change.Value.valueOf(it) }
            val after = r.after()[table.valueColumnName]?.asText()?.uppercase()?.let {  Change.Value.valueOf(it) }
            Change(table, if (after == null) before else null, after)
        }
        Assertions.assertEquals(expectedAsInsertsOrDeletes, actualAsInsertsOrDeletes)
    }

    fun assertCompletionReason(output: DebeziumComponent.Output, expected: CompletionReason) {
        Assertions.assertNotEquals(setOf<Any>(), output.completionReasons)
        Assertions.assertTrue(output.completionReasons.contains(expected),
                String.format("%s not found in %s", expected, output.completionReasons))
    }

    fun assertDataCountWithinBounds(lowerBoundInclusive: Int, output: DebeziumComponent.Output, upperBoundExclusive: Int) {
        Assertions.assertTrue(output.data.size >= lowerBoundInclusive,
                String.format("expected no less than %d records, obtained %d", lowerBoundInclusive, output.data.size))
        Assertions.assertTrue(output.data.size < upperBoundExclusive,
                String.format("expected less than %d records, obtained %d", upperBoundExclusive, output.data.size))
    }


}