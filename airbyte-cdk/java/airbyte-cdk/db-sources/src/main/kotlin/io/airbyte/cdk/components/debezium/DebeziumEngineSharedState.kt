/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.cdk.components.debezium

import com.google.common.collect.ImmutableList
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import java.lang.management.ManagementFactory
import java.lang.management.MemoryMXBean
import java.lang.management.MemoryUsage
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max
import kotlin.math.min

class DebeziumEngineSharedState {

    private val startEpochMilli = AtomicLong()
    private val numRecords = AtomicLong()
    private val lastRecordEpochMilli = AtomicLong()
    private val completionReasonsBitfield = AtomicLong()
    private val buffer: MutableList<BufferElement> = Collections.synchronizedList(ArrayList(1_000_000))
    internal var memoryUsageSupplier: MemoryUsageSupplier = JvmMemoryUsageSupplier()
    internal var clock: Clock = Clock.systemUTC()

    @JvmRecord
    internal data class BufferElement(val record: DebeziumComponent.Output.Record, val isWithinBounds: Boolean, val epochMilli: Long)

    fun reset() {
        startEpochMilli.set(Instant.now(clock).toEpochMilli())
        numRecords.set(0L)
        completionReasonsBitfield.set(0L)
        lastRecordEpochMilli.set(startEpochMilli.get())
        synchronized(buffer) {
            buffer.clear()
        }
    }

    val isComplete: Boolean
        get() = completionReasonsBitfield.get() != 0L

    fun add(record: DebeziumComponent.Output.Record, targets: DebeziumComponent.Input.CompletionTargets) {
        // Store current state before updating it.
        val now = Instant.now(clock)
        val previousNumRecords = numRecords()
        val elapsedSinceStart = Duration.between(startedAt(), now)
        val elapsedSinceLastRecord = Duration.between(lastRecordAt(), now)
        val isWithinBounds = targets.isWithinBounds.test(record)
        // Update buffer.
        addToBuffer(BufferElement(record, isWithinBounds, now.toEpochMilli()))
        // Time-based completion checks.
        if (targets.maxTime.minus(elapsedSinceStart).isNegative) {
            // We have spent enough time collecting records, shut down.
            addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_COLLECTED_LONG_ENOUGH)
        }
        if (previousNumRecords == 0L) {
            if (targets.maxWaitTimeForFirstRecord.minus(elapsedSinceLastRecord).isNegative) {
                // We have spend enough time waiting for the first record, shut down.
                addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_INITIAL_RECORD)
            }
        } else {
            if (targets.maxWaitTimeForSubsequentRecord.minus(elapsedSinceLastRecord).isNegative) {
                // We have spend enough time waiting for a subsequent record, shut down.
                addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD)
            }
        }
        // Other engine completion checks.
        if (!isWithinBounds) {
            // We exceeded the high-water mark, shut down.
            addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_EVENTS_OUT_OF_BOUNDS)
        }
        if (!record.isHeartbeat && previousNumRecords + 1 >= targets.maxRecords) {
            // We have collected enough records, shut down.
            addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_COLLECTED_ENOUGH_RECORDS)
        }
        if (record.kind() == DebeziumComponent.Output.Record.Kind.SNAPSHOT_COMPLETE) {
            // We were snapshotting and we finished the snapshot, shut down.
            addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_FINISHED_SNAPSHOTTING)
        }
        if (previousNumRecords % 1000 == 0L) {
            // Don't check memory usage too often, there is some overhead, even if it's not a lot.
            val heapUsage = memoryUsageSupplier.heapMemoryUsage()
            val maxHeapUsageRatio = min(1.0, max(0.0, 1.0 - targets.minUnusedHeapRatio))
            if (heapUsage.used > heapUsage.max * maxHeapUsageRatio) {
                // We are using more memory than we'd like, shut down.
                addCompletionReason(DebeziumComponent.Output.CompletionReason.MEMORY_PRESSURE)
            }
        }
    }

    private fun numRecords(): Long {
        return numRecords.get()
    }

    private fun startedAt(): Instant {
        return Instant.ofEpochMilli(startEpochMilli.get())
    }

    private fun lastRecordAt(): Instant {
        return Instant.ofEpochMilli(lastRecordEpochMilli.get())
    }

    private fun addToBuffer(e: BufferElement) {
        synchronized(buffer) {
            buffer.add(e)
        }
        if (!e.record.isHeartbeat) {
            numRecords.getAndIncrement()
            lastRecordEpochMilli.getAndUpdate { acc: Long -> java.lang.Long.max(acc, e.epochMilli) }
        }
    }

    fun addCompletionReason(reason: DebeziumComponent.Output.CompletionReason) {
        completionReasonsBitfield.getAndUpdate { acc: Long -> acc or (1L shl reason.ordinal) }
    }

    fun build(state: DebeziumComponent.State): DebeziumComponent.Output {
        val records = ImmutableList.builderWithExpectedSize<DebeziumComponent.Output.Record>(numRecords.get().toInt())
        val eventStats = DescriptiveStatistics()
        val recordStats = DescriptiveStatistics()
        val recordOutOfBoundsStats = DescriptiveStatistics()
        synchronized(buffer) {
            var previousEventEpochMilli = startEpochMilli.get()
            var previousRecordEpochMilli = startEpochMilli.get()
            var previousRecordOutOfBoundsEpochMilli = startEpochMilli.get()
            for ((record, isWithinBounds, epochMilli) in buffer) {
                eventStats.addValue(max(0.0, (epochMilli - previousEventEpochMilli).toDouble()))
                previousEventEpochMilli = epochMilli
                if (!record.isHeartbeat) {
                    records.add(record)
                    recordStats.addValue(max(0.0, (epochMilli - previousRecordEpochMilli).toDouble()))
                    previousRecordEpochMilli = epochMilli
                    if (!isWithinBounds) {
                        recordOutOfBoundsStats.addValue(max(0.0, (epochMilli - previousRecordOutOfBoundsEpochMilli).toDouble()))
                        previousRecordOutOfBoundsEpochMilli = epochMilli
                    }
                }
            }
        }
        val completionReasons = EnumSet.noneOf(DebeziumComponent.Output.CompletionReason::class.java)
        for (reason in DebeziumComponent.Output.CompletionReason.entries.toTypedArray()) {
            if ((completionReasonsBitfield.get() and (1L shl reason.ordinal)) != 0L) {
                completionReasons.add(reason)
            }
        }
        return DebeziumComponent.Output(
                records.build(),
                state,
                DebeziumComponent.Output.ExecutionSummary(
                        DebeziumComponent.Output.ExecutionSummary.LatencyStats(eventStats),
                        DebeziumComponent.Output.ExecutionSummary.LatencyStats(recordStats),
                        DebeziumComponent.Output.ExecutionSummary.LatencyStats(recordOutOfBoundsStats),
                        Duration.ofMillis(Instant.now(clock).toEpochMilli() - startEpochMilli.get())),
                completionReasons)
    }

    internal fun interface MemoryUsageSupplier {
        fun heapMemoryUsage(): MemoryUsage
    }

    private class JvmMemoryUsageSupplier : MemoryUsageSupplier {
        private val memoryBean: MemoryMXBean = ManagementFactory.getMemoryMXBean()
        override fun heapMemoryUsage(): MemoryUsage {
            return memoryBean.heapMemoryUsage
        }
    }
}
