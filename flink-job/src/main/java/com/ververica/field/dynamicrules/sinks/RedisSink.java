package com.ververica.field.dynamicrules.sinks;

import com.ververica.field.dynamicrules.metrics.MetricsInMemory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * @author lixingliang
 * @version v2.0.0
 * @date 2025/1/7
 */
public class RedisSink extends RichSinkFunction<MetricsInMemory> {
    private Jedis jedis;
    @Override
    public void open(Configuration parameters) throws Exception {
        // TODO 未来可改造为连接池，同时支持哨兵、集群模式
        ParameterTool globalJobParameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String redisHost = globalJobParameters.get("redis-host", "localhost");
        int redisPort = globalJobParameters.getInt("redis-port", 6379);
        jedis = new Jedis(redisHost, redisPort);
    }

    @Override
    public void invoke(MetricsInMemory metricsInMemory, Context context) throws Exception {
        String entityKey = metricsInMemory.getEntityKey();
        String metricCode = metricsInMemory.getMetricCode();
        String metricValue = metricsInMemory.getMetricValue();
        jedis.hset(entityKey, metricCode, metricValue);
        // TODO 设置key的超时时间
        // jedis.expire();
    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }

    public static DataStreamSink<MetricsInMemory> addRedisSink(DataStream<MetricsInMemory> redisResDataStream) {
        return redisResDataStream.addSink(new RedisSink());
    }
}
