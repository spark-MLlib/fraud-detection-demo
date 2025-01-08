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

import com.ververica.field.dynamicrules.accumulators.*;
import java.math.BigDecimal;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/* Collection of helper methods for Rules. */
public class RuleHelper {

  /* Picks and returns a new accumulator, based on the Rule's aggregator function type. */
  public static SimpleAccumulator<BigDecimal> getAggregator(Rule rule) {
    switch (rule.getAggregatorFunctionType()) {
      case COUNT:
      case SUM:
        return new BigDecimalCounter();
      case AVG:
        return new AverageAccumulator();
      case MAX:
        return new BigDecimalMaximum();
      case MIN:
        return new BigDecimalMinimum();
      default:
        throw new RuntimeException(
            "Unsupported aggregation function type: " + rule.getAggregatorFunctionType());
    }
  }

    public static SimpleAccumulator<BigDecimal> execAccumulatorAdd(Transaction transaction ,Rule rule, SimpleAccumulator<BigDecimal> accumulator) throws NoSuchFieldException, IllegalAccessException {
        BigDecimal aggregatedValue =
                FieldsExtractor.getBigDecimalByName(rule.getAggregateFieldName(), transaction);
        accumulator.add(aggregatedValue);
        return accumulator;
    }

    public static SimpleAccumulator<BigDecimal> ensureToCreateAccumulator(Rule rule, SimpleAccumulator<BigDecimal> accumulator) {
        if (accumulator == null) {
            accumulator = getAggregator(rule);
        }
        return accumulator;
    }
}
