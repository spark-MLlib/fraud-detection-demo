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

package com.ververica.field.dynamicrules;

import com.ververica.field.config.Config;
import com.ververica.field.config.Parameters;
import com.ververica.field.dynamicrules.metrics.MetricsInMemory;
import com.ververica.field.dynamicrules.functions.AverageAggregate;
import com.ververica.field.dynamicrules.functions.DynamicMetricsCalcFunction;
import com.ververica.field.dynamicrules.functions.DynamicKeyFunction;
import com.ververica.field.dynamicrules.sinks.CurrentRulesSink;
import com.ververica.field.dynamicrules.sinks.LatencySink;
import com.ververica.field.dynamicrules.sinks.MetricsSink;
import com.ververica.field.dynamicrules.sinks.RedisSink;
import com.ververica.field.dynamicrules.sources.RulesSource;
import com.ververica.field.dynamicrules.sources.TransactionsSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.ververica.field.config.Parameters.*;

@Slf4j
public class MetricsEvaluator {

  private final Config config;
  private String[] args;

  MetricsEvaluator(String[] args) {
    this.args = args;
    ParameterTool tool = ParameterTool.fromArgs(args);
    Parameters inputParams = new Parameters(tool);
    this.config = new Config(inputParams, STRING_PARAMS, INT_PARAMS, BOOL_PARAMS);
  }

  public void run() throws Exception {

    RulesSource.Type rulesSourceType = getRulesSourceType();

    boolean isLocal = config.get(LOCAL_EXECUTION);
    boolean enableCheckpoints = config.get(ENABLE_CHECKPOINTS);
    int checkpointsInterval = config.get(CHECKPOINT_INTERVAL);
    int minPauseBtwnCheckpoints = config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS);

    // Environment setup
    StreamExecutionEnvironment env = configureStreamExecutionEnvironment(rulesSourceType, isLocal);
    // 将客户端参数设置为flink全局参数, 方便在taskManager端使用
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(parameterTool);

    if (enableCheckpoints) {
      // TODO: 补全重要的checkpoint相关配置，例如状态后端、超时时间、是否对齐检查点、保留策略等
      env.enableCheckpointing(checkpointsInterval);
      env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBtwnCheckpoints);
    }

    // Streams setup
    DataStream<Rule> rulesUpdateStream = getRulesUpdateStream(env);
    DataStream<Transaction> transactions = getTransactionsStream(env);

    BroadcastStream<Rule> rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor);

    // Processing pipeline setup
    SingleOutputStreamOperator<Metric> metrics =
        transactions
            // TODO: 可选的waterMark
            .connect(rulesStream)
            .process(new DynamicKeyFunction())
            .uid("DynamicKeyFunction")
            .name("Dynamic Partitioning Function")
            .keyBy((keyed) -> keyed.getKey())
            .connect(rulesStream)
            .process(new DynamicMetricsCalcFunction())
            .uid("DynamicMetricsCalcFunction")
            .name("Dynamic Metrics Evaluation Function");

    DataStream<String> allMetricsEvaluations =
        metrics.getSideOutput(Descriptors.demoSinkTag);

    DataStream<Long> latency =
        metrics.getSideOutput(Descriptors.latencySinkTag);

    DataStream<Rule> currentRules =
        metrics.getSideOutput(Descriptors.currentRulesSinkTag);

    DataStream<MetricsInMemory> redisResDataStream = metrics.getSideOutput(Descriptors.redisSinkTag);
    RedisSink.addRedisSink(redisResDataStream);

    metrics.print().name("Metrics STDOUT Sink");
    allMetricsEvaluations.print().setParallelism(1).name("Metrics Evaluation Sink");

    DataStream<String> currentRulesJson = CurrentRulesSink.rulesStreamToJson(currentRules);
    currentRulesJson.print();

    DataStream<String> metricsJson = MetricsSink.metricsStreamToJson(metrics);
    DataStreamSink<String> metricsSink = MetricsSink.addMetricsSink(config, metricsJson);
    metricsSink.setParallelism(1).name("Metrics JSON Sink");

    DataStreamSink<String> currentRulesSink =
        CurrentRulesSink.addRulesSink(config, currentRulesJson);
    currentRulesSink.setParallelism(1);

    DataStream<String> latencies =
        latency
            .timeWindowAll(Time.seconds(10))
            .aggregate(new AverageAggregate())
            .map(String::valueOf);
    latencies.print().name("Latency STDOUT");

    DataStreamSink<String> latencySink = LatencySink.addLatencySink(config, latencies);
    latencySink.name("Latency Sink");

    env.execute("Real-time Metrics Engine");
  }

  private DataStream<Transaction> getTransactionsStream(StreamExecutionEnvironment env) {
    // Data stream setup
    int sourceParallelism = config.get(SOURCE_PARALLELISM);
    DataStream<String> transactionsStringsStream =
        TransactionsSource.initTransactionsSource(config, env)
            .name("Transactions Source")
            .setParallelism(sourceParallelism);
    DataStream<Transaction> transactionsStream =
        TransactionsSource.stringsStreamToTransactions(transactionsStringsStream);
    return transactionsStream.assignTimestampsAndWatermarks(
        new SimpleBoundedOutOfOrdernessTimestampExtractor<>(config.get(OUT_OF_ORDERNESS)));
  }

  private DataStream<Rule> getRulesUpdateStream(StreamExecutionEnvironment env) throws IOException {

    RulesSource.Type rulesSourceEnumType = getRulesSourceType();

    DataStream<String> rulesStrings =
        RulesSource.initRulesSource(config, env)
            .name(rulesSourceEnumType.getName())
            .setParallelism(1);
    return RulesSource.stringsStreamToRules(rulesStrings);
  }

  private RulesSource.Type getRulesSourceType() {
    String rulesSource = config.get(RULES_SOURCE);
    return RulesSource.Type.valueOf(rulesSource.toUpperCase());
  }

  private StreamExecutionEnvironment configureStreamExecutionEnvironment(
      RulesSource.Type rulesSourceEnumType, boolean isLocal) {
    Configuration flinkConfig = new Configuration();
    flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

    StreamExecutionEnvironment env =
        isLocal
            ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
            : StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    configureRestartStrategy(env, rulesSourceEnumType);
    return env;
  }

  private static class SimpleBoundedOutOfOrdernessTimestampExtractor<T extends Transaction>
      extends BoundedOutOfOrdernessTimestampExtractor<T> {

    public SimpleBoundedOutOfOrdernessTimestampExtractor(int outOfOrderdnessMillis) {
      super(Time.of(outOfOrderdnessMillis, TimeUnit.MILLISECONDS));
    }

    @Override
    public long extractTimestamp(T element) {
      return element.getEventTime();
    }
  }

  private void configureRestartStrategy(
      StreamExecutionEnvironment env, RulesSource.Type rulesSourceEnumType) {
    switch (rulesSourceEnumType) {
      case SOCKET:
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(
                10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
        break;
      case KAFKA:
        // Default - unlimited restart strategy.
        //        env.setRestartStrategy(RestartStrategies.noRestart());
    }
  }

  public static class Descriptors {
    public static final MapStateDescriptor<Integer, Rule> rulesDescriptor =
        new MapStateDescriptor<>(
            "rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Rule.class));

    public static final OutputTag<String> demoSinkTag = new OutputTag<String>("demo-sink") {};
    public static final OutputTag<Long> latencySinkTag = new OutputTag<Long>("latency-sink") {};
    public static final OutputTag<Rule> currentRulesSinkTag =
        new OutputTag<Rule>("current-rules-sink") {};
    public static final OutputTag<MetricsInMemory> redisSinkTag = new OutputTag<MetricsInMemory>("redis-sink") {};
  }
}
