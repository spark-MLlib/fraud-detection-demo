/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.field.dynamicrules.sinks;

import com.ververica.field.config.Config;
import com.ververica.field.dynamicrules.KafkaUtils;
import com.ververica.field.dynamicrules.Metric;
import com.ververica.field.dynamicrules.functions.JsonSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.io.IOException;
import java.util.Properties;

import static com.ververica.field.config.Parameters.*;

public class MetricsSink {

  public static DataStreamSink<String> addMetricsSink(Config config, DataStream<String> stream)
      throws IOException {

    String sinkType = config.get(METRICS_SINK);
    MetricsSink.Type metricsSinkType = MetricsSink.Type.valueOf(sinkType.toUpperCase());
    DataStreamSink<String> dataStreamSink;

    switch (metricsSinkType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initProducerProperties(config);
        String metricsTopic = config.get(METRICS_TOPIC);

        KafkaSink<String> kafkaSink =
            KafkaSink.<String>builder()
                .setKafkaProducerConfig(kafkaProps)
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.builder()
                        .setTopic(metricsTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        dataStreamSink = stream.sinkTo(kafkaSink);
        break;
      case STDOUT:
        dataStreamSink = stream.addSink(new PrintSinkFunction<>(true));
        break;
      default:
        throw new IllegalArgumentException(
            "Source \"" + metricsSinkType + "\" unknown. Known values are:" + Type.values());
    }
    return dataStreamSink;
  }

  public static DataStream<String> metricsStreamToJson(DataStream<Metric> alerts) {
    return alerts.flatMap(new JsonSerializer<>(Metric.class)).name("Alerts Deserialization");
  }

  public enum Type {
    KAFKA("Metrics Sink (Kafka)"),
    STDOUT("Metrics Sink (Std. Out)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
