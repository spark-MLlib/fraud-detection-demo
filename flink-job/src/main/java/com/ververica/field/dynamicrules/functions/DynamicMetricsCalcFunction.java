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

package com.ververica.field.dynamicrules.functions;

import com.ververica.field.dynamicrules.*;
import com.ververica.field.dynamicrules.Rule.ControlType;
import com.ververica.field.dynamicrules.Rule.RuleState;
import com.ververica.field.dynamicrules.RulesEvaluator.Descriptors;
import com.ververica.field.windowing.assigners.WindowAssigner;
import com.ververica.field.windowing.windows.TimeWindow;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.ververica.field.dynamicrules.functions.ProcessingUtils.addToStateValuesSet;
import static com.ververica.field.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;

/** Implements main rule evaluation and alerting logic. */
@Slf4j
public class DynamicMetricsCalcFunction
    extends KeyedBroadcastProcessFunction<
        String, Keyed<Transaction, String, Integer>, Rule, Metric> {

  private static final String COUNT = "COUNT_FLINK";
  private static final String COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK";

  private static int WIDEST_RULE_KEY = Integer.MIN_VALUE;
  private static int CLEAR_STATE_COMMAND_KEY = Integer.MIN_VALUE + 1;

  private transient MapState<Long, Set<Transaction>> windowState;
  private Meter alertMeter;

  // TODO: 1.考虑做状态生命周期的统一管理
  private MapStateDescriptor<Long, Set<Transaction>> windowStateDescriptor =
      new MapStateDescriptor<>(
          "windowState",
          BasicTypeInfo.LONG_TYPE_INFO,
          TypeInformation.of(new TypeHint<Set<Transaction>>() {}));

    private transient MapState<String, SimpleAccumulator<BigDecimal>> growthWindowState;
    private final MapStateDescriptor<String, SimpleAccumulator<BigDecimal>> growthWindowStateDescriptor =
            new MapStateDescriptor<>(
                    "growthWindowState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<SimpleAccumulator<BigDecimal>>() {}));

    private transient ValueState<Long> growthWindowStatecleanupTime;

    @Override
    public void open(Configuration parameters) {

    windowState = getRuntimeContext().getMapState(windowStateDescriptor);
    growthWindowState = getRuntimeContext().getMapState(growthWindowStateDescriptor);
    alertMeter = new MeterView(60);
    getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
    growthWindowStatecleanupTime = getRuntimeContext().getState(
            new ValueStateDescriptor<>("growthWindowStateClearedTime", Types.LONG));
  }

  @Override
  public void processElement(
      Keyed<Transaction, String, Integer> value, ReadOnlyContext ctx, Collector<Metric> out)
      throws Exception {
    Rule rule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(value.getId());
    if (noRuleAvailable(rule)) {
      log.error("Rule with ID {} does not exist", value.getId());
      return;
    }
    if (Rule.WindowType.GROWTH_WINDOW == rule.getWindowType()) {
        long currentEventTime = value.getWrapped().getEventTime();

        if (rule.getRuleState() == RuleState.ACTIVE) {
            TimeWindow timeWindow = WindowAssigner.assignWindow(currentEventTime, rule);

            // 注册状态清理时间
            long cleanupTime = timeWindow.maxTimestamp();
            // 更新增长窗口状态清除时间
            Long growthWindowStatecleanupTimeValue = growthWindowStatecleanupTime.value();
            if (growthWindowStatecleanupTimeValue == null || growthWindowStatecleanupTimeValue < cleanupTime) {
                growthWindowStatecleanupTime.update(cleanupTime);
            }

            ctx.timerService().registerEventTimeTimer(cleanupTime);

            // Calculate the aggregate value
            SimpleAccumulator<BigDecimal> aggregator;
            // TODO 增长窗口的key计划暂定为 ruleId + groupId,未来可能变化
            String key = rule.getRuleId() + "|" +value.getKey();
            aggregator = growthWindowState.get(key);
            aggregator = RuleHelper.ensureToCreateAccumulator(rule,aggregator);
            aggregator.add(BigDecimal.ONE);
            growthWindowState.put(key, aggregator);

            BigDecimal aggregateResult = aggregator.getLocalValue();

            ctx.output(
                    Descriptors.demoSinkTag,
                    "Rule "
                            + rule.getRuleId()
                            + " | "
                            + value.getKey()
                            + " | "
                            + rule.getWindowType()
                            + " | "
                            + rule.getAggregatorFunctionType()
                            + " : "
                            + aggregateResult.toString()
                            + " -> "
                            + "DEFAULT");
            out.collect(
                    new Metric<>(
                            rule.getRuleId(), rule, value.getKey(), value.getWrapped(), aggregateResult));
        }
    } else {
        // Add Transaction to state
        long currentEventTime = value.getWrapped().getEventTime();
        addToStateValuesSet(windowState, currentEventTime, value.getWrapped());

        long ingestionTime = value.getWrapped().getIngestionTimestamp();
        ctx.output(Descriptors.latencySinkTag, System.currentTimeMillis() - ingestionTime);


        // Calculate the aggregate value
        if (noRuleAvailable(rule)) {
            log.error("Rule with ID {} does not exist", value.getId());
            return;
        }
        if (rule.getRuleState() == RuleState.ACTIVE) {
            TimeWindow timeWindow = WindowAssigner.assignWindow(currentEventTime, rule);
            Long windowStartForEvent = timeWindow.getStart();

            long cleanupTime = (currentEventTime / 1000) * 1000;
            ctx.timerService().registerEventTimeTimer(cleanupTime);

            SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
            for (Long stateEventTime : windowState.keys()) {
                if (isStateValueInWindow(stateEventTime, windowStartForEvent, currentEventTime)) {
                    aggregateValuesInState(stateEventTime, aggregator, rule);
                }
            }
            BigDecimal aggregateResult = aggregator.getLocalValue();

            ctx.output(
                Descriptors.demoSinkTag,
                "Rule "
                        + rule.getRuleId()
                        + " | "
                        + value.getKey()
                        + " : "
                        + aggregateResult.toString()
                        + " -> "
                        + "DEFAULT");

            out.collect(
                    new Metric<>(
                            rule.getRuleId(), rule, value.getKey(), value.getWrapped(), aggregateResult));
        }
    }
  }

  @Override
  public void processBroadcastElement(Rule rule, Context ctx, Collector<Metric> out)
      throws Exception {
    log.info("{}", rule);
    BroadcastState<Integer, Rule> broadcastState =
        ctx.getBroadcastState(Descriptors.rulesDescriptor);
    handleRuleBroadcast(rule, broadcastState);
    updateWidestWindowRule(rule, broadcastState);
    if (rule.getRuleState() == RuleState.CONTROL) {
      handleControlCommand(rule, broadcastState, ctx);
    }
  }

  private void handleControlCommand(
      Rule command, BroadcastState<Integer, Rule> rulesState, Context ctx) throws Exception {
    ControlType controlType = command.getControlType();
    switch (controlType) {
      case EXPORT_RULES_CURRENT:
        for (Entry<Integer, Rule> entry : rulesState.entries()) {
          ctx.output(Descriptors.currentRulesSinkTag, entry.getValue());
        }
        break;
      case CLEAR_STATE_ALL:
        ctx.applyToKeyedState(windowStateDescriptor, (key, state) -> state.clear());
        break;
      case CLEAR_STATE_ALL_STOP:
        rulesState.remove(CLEAR_STATE_COMMAND_KEY);
        break;
      case DELETE_RULES_ALL:
        Iterator<Entry<Integer, Rule>> entriesIterator = rulesState.iterator();
        while (entriesIterator.hasNext()) {
          Entry<Integer, Rule> ruleEntry = entriesIterator.next();
          rulesState.remove(ruleEntry.getKey());
          log.info("Removed Rule {}", ruleEntry.getValue());
        }
        break;
    }
  }

  private boolean isStateValueInWindow(
      Long stateEventTime, Long windowStartForEvent, long currentEventTime) {
    return stateEventTime >= windowStartForEvent && stateEventTime <= currentEventTime;
  }

  private void aggregateValuesInState(
      Long stateEventTime, SimpleAccumulator<BigDecimal> aggregator, Rule rule) throws Exception {
    Set<Transaction> inWindow = windowState.get(stateEventTime);
    if (COUNT.equals(rule.getAggregateFieldName())
        || COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
      for (Transaction event : inWindow) {
        aggregator.add(BigDecimal.ONE);
      }
    } else {
      for (Transaction event : inWindow) {
        BigDecimal aggregatedValue =
            FieldsExtractor.getBigDecimalByName(rule.getAggregateFieldName(), event);
        aggregator.add(aggregatedValue);
      }
    }
  }

  private boolean noRuleAvailable(Rule rule) {
    // This could happen if the BroadcastState in this CoProcessFunction was updated after it was
    // updated and used in `DynamicKeyFunction`
    if (rule == null) {
      return true;
    }
    return false;
  }

  private void updateWidestWindowRule(Rule rule, BroadcastState<Integer, Rule> broadcastState)
      throws Exception {
    Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);

    if (rule.getRuleState() != RuleState.ACTIVE) {
      return;
    }

    if (widestWindowRule == null) {
      broadcastState.put(WIDEST_RULE_KEY, rule);
      return;
    }

    if (widestWindowRule.getWindowMillis() < rule.getWindowMillis()) {
      broadcastState.put(WIDEST_RULE_KEY, rule);
    }
  }

  @Override
  public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Metric> out)
      throws Exception {

    Rule widestWindowRule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(WIDEST_RULE_KEY);

      // 清除增长窗口的状态
      if (Rule.WindowType.GROWTH_WINDOW == widestWindowRule.getWindowType()) {
          Long growthWindowStatecleanupTimeValue = growthWindowStatecleanupTime.value();
          if (growthWindowStatecleanupTimeValue != null && timestamp >= growthWindowStatecleanupTimeValue) {
              log.info("开始清除增长窗口状态，清理之前值为:{}", growthWindowState.entries().iterator().next().getValue());
              growthWindowState.clear();
          }
      } else { // 清除非增长窗口的状态
          Optional<Long> cleanupEventTimeWindow =
              Optional.ofNullable(widestWindowRule).map(Rule::getWindowMillis);
          Optional<Long> cleanupEventTimeThreshold =
              cleanupEventTimeWindow.map(window -> timestamp - window);
          // TODO 在数据量特别大时，极可能存在性能风险
          cleanupEventTimeThreshold.ifPresent(this::evictAgedElementsFromWindow);
      }
  }

  private void evictAgedElementsFromWindow(Long threshold) {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        Long stateEventTime = keys.next();
        if (stateEventTime < threshold) {
          keys.remove();
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void evictAllStateElements() {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        keys.next();
        keys.remove();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
