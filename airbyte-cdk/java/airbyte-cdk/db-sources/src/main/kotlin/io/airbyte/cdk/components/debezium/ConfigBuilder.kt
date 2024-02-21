/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.cdk.components.debezium

import io.debezium.spi.common.ReplacementFunction
import io.debezium.storage.file.history.FileSchemaHistory
import org.apache.kafka.connect.connector.Connector
import org.apache.kafka.connect.storage.FileOffsetBackingStore
import java.time.Duration
import java.util.*

open class ConfigBuilder<B : ConfigBuilder<B>> {

    protected val props: Properties = Properties()

    init {
        // default values from debezium CommonConnectorConfig
        props.setProperty("max.batch.size", "2048")
        props.setProperty("max.queue.size", "8192")

        // Disabling retries because debezium startup time might exceed our 60-second wait limit
        // The maximum number of retries on connection errors before failing (-1 = no limit, 0 = disabled, >
        // 0 = num of retries).
        props.setProperty("errors.max.retries", "0")

        // This property must be strictly less than errors.retry.delay.max.ms
        // (https://github.com/debezium/debezium/blob/bcc7d49519a4f07d123c616cfa45cd6268def0b9/debezium-core/src/main/java/io/debezium/util/DelayStrategy.java#L135)
        props.setProperty("errors.retry.delay.initial.ms", "299")
        props.setProperty("errors.retry.delay.max.ms", "300")

        // https://debezium.io/documentation/reference/2.2/configuration/avro.html
        props.setProperty("key.converter.schemas.enable", "false")
        props.setProperty("value.converter.schemas.enable", "false")

        // By default "decimal.handing.mode=precise" which's caused returning this value as a binary.
        // The "double" type may cause a loss of precision, so set Debezium's config to store it as a String
        // explicitly in its Kafka messages for more details see:
        // https://debezium.io/documentation/reference/2.2/connectors/postgresql.html#postgresql-decimal-types
        // https://debezium.io/documentation/faq/#how_to_retrieve_decimal_field_from_binary_representation
        props.setProperty("decimal.handling.mode", "string")

        props.setProperty("offset.storage", FileOffsetBackingStore::class.java.canonicalName)
        props.setProperty("offset.flush.interval.ms", "1000") // todo: make this longer

        props.setProperty("debezium.embedded.shutdown.pause.before.interrupt.ms", "10000")
    }

    fun build(): DebeziumComponent.Input.Config {
        return DebeziumComponent.Input.Config(props.clone() as Properties)
    }

    @Suppress("UNCHECKED_CAST")
    private fun self(): B {
        return this as B
    }

    fun withSchemaHistory(): B {
        props.setProperty("schema.history.internal", FileSchemaHistory::class.java.canonicalName)
        props.setProperty("schema.history.internal.store.only.captured.databases.ddl", "true")
        return self()
    }

    fun withDebeziumName(name: String): B {
        props.setProperty("name", name)
        // WARNING:
        // Never change the value of this otherwise all the connectors would start syncing from scratch.
        props.setProperty("topic.prefix", sanitizeTopicPrefix(name))
        return self()
    }

    fun withConnector(connectorClass: Class<out Connector>): B {
        props.setProperty("connector.class", connectorClass.canonicalName)
        return self()
    }

    fun withHeartbeats(heartbeatInterval: Duration): B {
        props.setProperty("heartbeat.interval.ms", heartbeatInterval.toMillis().toString())
        return self()
    }

    fun with(key: String, value: String): B {
        props.setProperty(key, value)
        return self()
    }

    fun with(properties: Map<*, *>): B {
        props.putAll(properties)
        return self()
    }

    private fun sanitizeTopicPrefix(topicName: String): String {
        val sanitizedNameBuilder = StringBuilder(topicName.length)
        for (c in topicName) {
            sanitizedNameBuilder.append(sanitizedChar(c))
        }
        return sanitizedNameBuilder.toString()
    }

    // We need to keep the validation rule the same as debezium engine, which is defined here:
    // https://github.com/debezium/debezium/blob/c51ef3099a688efb41204702d3aa6d4722bb4825/debezium-core/src/main/java/io/debezium/schema/AbstractTopicNamingStrategy.java#L178
    private fun sanitizedChar(c: Char): String = when (c) {
        '.', '_', '-', in 'A'..'Z', in 'a'..'z', in '0'..'9' -> c.toString()
        else -> ReplacementFunction.UNDERSCORE_REPLACEMENT.replace(c)
    }
}
