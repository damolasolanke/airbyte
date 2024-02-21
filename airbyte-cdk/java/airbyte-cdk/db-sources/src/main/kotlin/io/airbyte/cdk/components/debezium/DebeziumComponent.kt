/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.cdk.components.debezium

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import io.debezium.relational.history.HistoryRecord
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.kafka.connect.source.SourceRecord
import java.time.Duration
import java.util.*
import java.util.function.Predicate

fun interface DebeziumComponent {
    fun collect(input: Input): Output

    @JvmRecord
    data class Input(val config: Config, val state: State, val completionTargets: CompletionTargets) {

        @JvmRecord
        data class Config(val properties: Properties) {

            val debeziumName: String
                get() = properties.getProperty("name")
        }

        @JvmRecord
        data class CompletionTargets(val isWithinBounds: Predicate<Output.Record>,
                                     val maxRecords: Long,
                                     val maxTime: Duration,
                                     val maxWaitTimeForFirstRecord: Duration,
                                     val maxWaitTimeForSubsequentRecord: Duration,
                                     val minUnusedHeapRatio: Double)
    }

    @JvmRecord
    data class State(val offset: Offset, val schema: Optional<Schema>) {

        @JvmRecord
        data class Offset(val debeziumOffset: Map<JsonNode, JsonNode>)

        @JvmRecord
        data class Schema(val debeziumSchemaHistory: List<HistoryRecord>)

    }

    @JvmRecord
    data class Output(val data: List<Record>,
                      val state: State,
                      val executionSummary: ExecutionSummary,
                      val completionReasons: Set<CompletionReason>) {
        @JvmRecord
        data class Record(val debeziumEventValue: JsonNode, val debeziumSourceRecord: Optional<SourceRecord>) {
            val isHeartbeat: Boolean
                get() = kind() == Kind.HEARTBEAT

            fun before(): JsonNode {
                return element("before")
            }

            fun after(): JsonNode {
                return element("after")
            }

            fun source(): JsonNode {
                return element("source")
            }

            fun element(fieldName: String?): JsonNode {
                if (!debeziumEventValue.has(fieldName)) {
                    return NullNode.getInstance()
                }
                return debeziumEventValue[fieldName]
            }

            fun kind(): Kind {
                val source = source()
                if (source.isNull) {
                    return Kind.HEARTBEAT
                }
                val snapshot = source["snapshot"] ?: return Kind.CHANGE
                return when (snapshot.asText().lowercase(Locale.getDefault())) {
                    "false" -> Kind.CHANGE
                    "last" -> Kind.SNAPSHOT_COMPLETE
                    else -> Kind.SNAPSHOT_ONGOING
                }
            }

            enum class Kind {
                HEARTBEAT,
                CHANGE,
                SNAPSHOT_ONGOING,
                SNAPSHOT_COMPLETE,
            }
        }

        @JvmRecord
        data class ExecutionSummary(val events: LatencyStats,
                                    val records: LatencyStats,
                                    val recordsOutOfBounds: LatencyStats,
                                    val collectDuration: Duration) {
            @JvmRecord
            data class LatencyStats(val elapsedSincePreviousMilli: DescriptiveStatistics) {

                fun count(): Long {
                    return elapsedSincePreviousMilli.n
                }

                fun first(): Duration {
                    return durationStat(if (count() == 0L) Double.NaN else elapsedSincePreviousMilli.getElement(0))
                }

                fun last(): Duration {
                    return durationStat(if (count() == 0L) Double.NaN else elapsedSincePreviousMilli.getElement(count().toInt() - 1))
                }

                fun min(): Duration {
                    return durationStat(elapsedSincePreviousMilli.min)
                }

                fun p01(): Duration {
                    return durationStat(elapsedSincePreviousMilli.getPercentile(0.01))
                }

                fun p05(): Duration {
                    return durationStat(elapsedSincePreviousMilli.getPercentile(0.05))
                }

                fun p10(): Duration {
                    return durationStat(elapsedSincePreviousMilli.getPercentile(0.10))
                }

                fun p25(): Duration {
                    return durationStat(elapsedSincePreviousMilli.getPercentile(0.25))
                }

                fun median(): Duration {
                    return durationStat(elapsedSincePreviousMilli.getPercentile(0.50))
                }

                fun p75(): Duration {
                    return durationStat(elapsedSincePreviousMilli.getPercentile(0.75))
                }

                fun p90(): Duration {
                    return durationStat(elapsedSincePreviousMilli.getPercentile(0.90))
                }

                fun p95(): Duration {
                    return durationStat(elapsedSincePreviousMilli.getPercentile(0.95))
                }

                fun p99(): Duration {
                    return durationStat(elapsedSincePreviousMilli.getPercentile(0.99))
                }

                fun max(): Duration {
                    return durationStat(elapsedSincePreviousMilli.max)
                }

                private fun durationStat(stat: Double): Duration {
                    return if (stat.isNaN()) Duration.ZERO else Duration.ofMillis(stat.toLong())
                }
            }
        }

        enum class CompletionReason {
            HAS_FINISHED_SNAPSHOTTING,
            HAS_EVENTS_OUT_OF_BOUNDS,
            HAS_COLLECTED_ENOUGH_RECORDS,
            HAS_COLLECTED_LONG_ENOUGH,
            HAS_WAITED_LONG_ENOUGH_FOR_INITIAL_RECORD,
            HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD,
            MEMORY_PRESSURE,
        }
    }
}
