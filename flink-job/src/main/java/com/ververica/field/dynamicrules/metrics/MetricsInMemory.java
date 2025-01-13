package com.ververica.field.dynamicrules.metrics;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lixingliang
 * @version v2.0.0
 * @date 2025/1/7
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MetricsInMemory {
    String entityKey;
    String metricCode;
    String metricValue;
}
