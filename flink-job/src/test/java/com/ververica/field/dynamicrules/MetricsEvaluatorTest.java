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

import com.ververica.field.dynamicrules.RulesEvaluator.Descriptors;
import com.ververica.field.dynamicrules.functions.DynamicKeyFunction;
import com.ververica.field.dynamicrules.functions.DynamicMetricsCalcFunction;
import com.ververica.field.dynamicrules.util.AssertUtils;
import com.ververica.field.dynamicrules.util.BroadcastStreamKeyedOperatorTestHarness;
import com.ververica.field.dynamicrules.util.BroadcastStreamNonKeyedOperatorTestHarness;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Test;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests for the {@link MetricsEvaluator}.
 */
public class MetricsEvaluatorTest {

    @Test
    public void shouldProduceKeyedOutput() throws Exception {
        RuleParser ruleParser = new RuleParser();
        Rule rule1 =
                ruleParser.fromString("{\"ruleId\":\"1\",\"aggregateFieldName\":\"paymentAmount\",\"aggregatorFunctionType\":\"COUNT\",\"windowType\":\"GROWTH_WINDOW\",\"groupingKeyNames\":[\"beneficiaryId\"],\"limit\":\"1000\",\"limitOperatorType\":\"GREATER\",\"ruleState\":\"ACTIVE\",\"windowMinutes\":\"1440\",\"metricsOutTags\":[\"InMemoryDB\"],\"metricsCode\":\"TotalNumberCallRing\"}");
        Transaction event1 = Transaction.parseJson("{\"transactionId\":8337461437400088757,\"eventTime\":1737360000000,\"payeeId\":76389,\"beneficiaryId\":58866,\"paymentAmount\":18,\"paymentType\":\"CSH\"}");
        try (BroadcastStreamNonKeyedOperatorTestHarness<
                Transaction, Rule, Keyed<Transaction, String, Integer>>
                     testHarness =
                     BroadcastStreamNonKeyedOperatorTestHarness.getInitializedTestHarness(
                             new DynamicKeyFunction(), Descriptors.rulesDescriptor)) {

            testHarness.processElement2(new StreamRecord<>(rule1, 12L));
            testHarness.processElement1(new StreamRecord<>(event1, 15L));

            Queue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
            expectedOutput.add(
                    new StreamRecord<>(new Keyed<>(event1, "{beneficiaryId=58866}", 1), 15L));

            TestHarnessUtil.assertOutputEquals(
                    "Wrong dynamically keyed output", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    public void shouldStoreRulesInBroadcastStateDuringDynamicKeying() throws Exception {
        RuleParser ruleParser = new RuleParser();
        Rule rule1 = ruleParser.fromString("{\"ruleId\":\"1\",\"aggregateFieldName\":\"paymentAmount\",\"aggregatorFunctionType\":\"COUNT\",\"windowType\":\"GROWTH_WINDOW\",\"groupingKeyNames\":[\"payeeId\",\"beneficiaryId\"],\"limit\":\"20000000\",\"limitOperatorType\":\"GREATER\",\"ruleState\":\"ACTIVE\",\"windowMinutes\":\"1440\",\"metricsOutTags\":[\"MessageQueue\"],\"metricsCode\":\"TotalNumberCallRing\"}");

        try (BroadcastStreamNonKeyedOperatorTestHarness<
                Transaction, Rule, Keyed<Transaction, String, Integer>>
                     testHarness =
                     BroadcastStreamNonKeyedOperatorTestHarness.getInitializedTestHarness(
                             new DynamicKeyFunction(), Descriptors.rulesDescriptor)) {

            testHarness.processElement2(new StreamRecord<>(rule1, 1L));

            BroadcastState<Integer, Rule> broadcastState =
                    testHarness.getBroadcastState(Descriptors.rulesDescriptor);

            Map<Integer, Rule> expectedState = new HashMap<>();
            expectedState.put(rule1.getRuleId(), rule1);

            AssertUtils.assertEquals(broadcastState, expectedState, "Output was not correct.");
        }
    }

    @Test
    public void shouldHandleEventsCorrectlyInGrowthWindow() throws Exception {
        // 使用增长窗口
        RuleParser ruleParser = new RuleParser();
        Rule rule1 =
                ruleParser.fromString("{\"ruleId\":\"1\",\"aggregateFieldName\":\"paymentAmount\",\"aggregatorFunctionType\":\"COUNT\",\"windowType\":\"GROWTH_WINDOW\",\"groupingKeyNames\":[\"payeeId\",\"beneficiaryId\"],\"limit\":\"20000000\",\"limitOperatorType\":\"GREATER\",\"ruleState\":\"ACTIVE\",\"windowMinutes\":\"1440\",\"metricsOutTags\":[\"MessageQueue\"],\"metricsCode\":\"TotalNumberCallRing\"}");
        // 2025-01-20 16:00:00
        Transaction event1 = Transaction.parseJson("{\"transactionId\":8337461437400088757,\"eventTime\":1737360000000,\"payeeId\":76389,\"beneficiaryId\":58866,\"paymentAmount\":18,\"paymentType\":\"CSH\",\"ingestionTimestamp\":1}");
        // 2025-01-20 16:30:00
        Transaction event2 = Transaction.parseJson("{\"transactionId\":8337461437400088757,\"eventTime\":1737361800000,\"payeeId\":76389,\"beneficiaryId\":58866,\"paymentAmount\":18,\"paymentType\":\"CSH\",\"ingestionTimestamp\":1}");

        Keyed<Transaction, String, Integer> keyed1 = new Keyed<>(event1, "CSH", 1);
        Keyed<Transaction, String, Integer> keyed2 = new Keyed<>(event2, "CSH", 1);

        try (BroadcastStreamKeyedOperatorTestHarness<
                String, Keyed<Transaction, String, Integer>, Rule, Metric>
                     testHarness =
                     BroadcastStreamKeyedOperatorTestHarness.getInitializedTestHarness(
                             new DynamicMetricsCalcFunction(),
                             in -> (in.getKey()),
                             null,
                             BasicTypeInfo.STRING_TYPE_INFO,
                             Descriptors.rulesDescriptor)) {

            testHarness.processElement2(new StreamRecord<>(rule1, 12L));

            testHarness.processElement1(new StreamRecord<>(keyed1, 15L));
            testHarness.processElement1(new StreamRecord<>(keyed2, 16L));

            ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

            Metric<Transaction, BigDecimal> metric1 = new Metric<>(rule1.getRuleId(), rule1, "CSH", event1, new BigDecimal(1));

            Metric<Transaction, BigDecimal> metric2 = new Metric<>(rule1.getRuleId(), rule1, "CSH", event2, new BigDecimal(2));

            expectedOutput.add(new StreamRecord<>(metric1, 15L));
            expectedOutput.add(new StreamRecord<>(metric2, 16L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct.", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    public void shouldTriggerCleanupStateCorrectlyInGrowthWindow() throws Exception {
        RuleParser ruleParser = new RuleParser();
        Rule rule1 =
                ruleParser.fromString("{\"ruleId\":\"1\",\"aggregateFieldName\":\"paymentAmount\",\"aggregatorFunctionType\":\"COUNT\",\"windowType\":\"GROWTH_WINDOW\",\"groupingKeyNames\":[\"payeeId\",\"beneficiaryId\"],\"limit\":\"20000000\",\"limitOperatorType\":\"GREATER\",\"ruleState\":\"ACTIVE\",\"windowMinutes\":\"1440\",\"metricsOutTags\":[\"MessageQueue\"],\"metricsCode\":\"TotalNumberCallRing\"}");

        // 2025-01-20 16:00:00
        Transaction event1 = Transaction.parseJson("{\"transactionId\":8337461437400088757,\"eventTime\":1737360000000,\"payeeId\":76389,\"beneficiaryId\":58866,\"paymentAmount\":18,\"paymentType\":\"CSH\",\"ingestionTimestamp\":1}");
        // 2025-01-20 16:30:00
        Transaction event2 = Transaction.parseJson("{\"transactionId\":8337461437400088757,\"eventTime\":1737361800000,\"payeeId\":76389,\"beneficiaryId\":58866,\"paymentAmount\":18,\"paymentType\":\"CSH\",\"ingestionTimestamp\":1}");
        // 2025-01-21 16:00:00
        Transaction event3 = Transaction.parseJson("{\"transactionId\":8337461437400088757,\"eventTime\":1737446400000,\"payeeId\":76389,\"beneficiaryId\":58866,\"paymentAmount\":18,\"paymentType\":\"CSH\",\"ingestionTimestamp\":1}");
        // 2025-01-21 16:30:00
        Transaction event4 = Transaction.parseJson("{\"transactionId\":8337461437400088757,\"eventTime\":1737361800000,\"payeeId\":76389,\"beneficiaryId\":58866,\"paymentAmount\":18,\"paymentType\":\"CSH\",\"ingestionTimestamp\":1}");

        Keyed<Transaction, String, Integer> keyed1 = new Keyed<>(event1, "CSH", 1);
        Keyed<Transaction, String, Integer> keyed2 = new Keyed<>(event2, "CSH", 1);
        Keyed<Transaction, String, Integer> keyed3 = new Keyed<>(event3, "CSH", 1);
        Keyed<Transaction, String, Integer> keyed4 = new Keyed<>(event4, "CSH", 1);

        try (BroadcastStreamKeyedOperatorTestHarness<
                String, Keyed<Transaction, String, Integer>, Rule, Metric>
                     testHarness =
                     BroadcastStreamKeyedOperatorTestHarness.getInitializedTestHarness(
                             new DynamicMetricsCalcFunction(),
                             in -> (in.getKey()),
                             null,
                             BasicTypeInfo.STRING_TYPE_INFO,
                             Descriptors.rulesDescriptor)) {

            testHarness.processElement2(new StreamRecord<>(rule1, 1L));

            testHarness.processElement1(toStreamRecord(keyed1));
            testHarness.watermark(event1.getEventTime());

            testHarness.processElement1(toStreamRecord(keyed2));
            testHarness.watermark(event2.getEventTime());

            testHarness.processElement1(toStreamRecord(keyed3));
            testHarness.watermark(event3.getEventTime());

            testHarness.processElement1(toStreamRecord(keyed4));
            testHarness.watermark(event4.getEventTime());

            ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
            Metric<Transaction, BigDecimal> metric1 = new Metric<>(rule1.getRuleId(), rule1, "CSH", event1, new BigDecimal(1));
            Metric<Transaction, BigDecimal> metric2 = new Metric<>(rule1.getRuleId(), rule1, "CSH", event2, new BigDecimal(2));
            // flink水印机制决定，水印会比事件时间滞后一次
            Metric<Transaction, BigDecimal> metric3 = new Metric<>(rule1.getRuleId(), rule1, "CSH", event3, new BigDecimal(3));
            Metric<Transaction, BigDecimal> metric4 = new Metric<>(rule1.getRuleId(), rule1, "CSH", event4, new BigDecimal(1));

            expectedOutput.add(new StreamRecord<>(metric1, event1.getEventTime()));
            expectedOutput.add(new StreamRecord<>(metric2, event2.getEventTime()));
            expectedOutput.add(new StreamRecord<>(metric3, event3.getEventTime()));
            expectedOutput.add(new StreamRecord<>(metric4, event4.getEventTime()));

            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct.", expectedOutput, filterOutWatermarks(testHarness.getOutput()));
        }
    }

    private StreamRecord<Keyed<Transaction, String, Integer>> toStreamRecord(
            Keyed<Transaction, String, Integer> keyed) {
        return new StreamRecord<>(keyed, keyed.getWrapped().getEventTime());
    }

    private Queue<Object> filterOutWatermarks(Queue<Object> in) {
        Queue<Object> out = new LinkedList<>();
        for (Object o : in) {
            if (!(o instanceof Watermark)) {
                out.add(o);
            }
        }
        return out;
    }
}
