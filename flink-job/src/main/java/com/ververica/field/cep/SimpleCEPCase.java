package com.ververica.field.cep;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.ververica.field.dynamicrules.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author lixingliang
 * @version v2.0.0
 * @date 2025/1/16
 */
public class SimpleCEPCase {
    public static void main(String[] args) throws Exception {
//        ctiCepCase();
        ctiCepCaseNPattern1Stream();
//        ctiCepCasePatternGroupInProcessingTime();
    }

    public static void ctiCepCase() throws Exception {
        // 创建 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<Event> input = env.addSource(new EventSourceFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Event>forMonotonousTimestamps() // 对事件使用单调递增的时间戳
                        .<Event>forBoundedOutOfOrderness(Duration.ofMinutes(3)) // 允许乱序
                        .withTimestampAssigner(new MySerializableTimestampAssigner()));

        // 设置可选的分区
        KeyedStream<Event, String> eventKeyedStream = input.keyBy(Event::getId);

        // 设置事件匹配策略
        Pattern<Event, ?> eventMatchingStrategy = createEventMatchingStrategy();

        // 设置最大有效时间间隔
        Pattern<Event, ?> pattern = eventMatchingStrategy.within(Time.minutes(5));

        // 使用 CEP 识别模式，返回 PatternStream 类型
        PatternStream<Event> patternStream = CEP.pattern(eventKeyedStream, pattern);

        // 处理匹配和未匹配的数据
        SingleOutputStreamOperator<String> result = patternStream.process(new MyPatternProcessFunction());

        // 打印匹配成功项
        result.print().name("Matched Successes");

        // 使用侧输出流，打印匹配失败项
        SideOutputDataStream<String> timeoutWasNotMatchedOutput = result.getSideOutput(Descriptors.timeoutWasNotMatchedOutputTag);
        timeoutWasNotMatchedOutput.print().name("Timeout Matched Failed");

        Pattern<Event, ?> eventMatchingStrategy1 = createEventMatchingStrategy1();
        Pattern<Event, ?> pattern1 = eventMatchingStrategy1.within(Time.minutes(5));
        PatternStream<Event> patternStream1 = CEP.pattern(eventKeyedStream, pattern1);
        SingleOutputStreamOperator<String> result1 = patternStream1.process(new MyPatternProcessFunction());
        result1.print().name("Matched Successes1");
        SideOutputDataStream<String> timeoutWasNotMatchedOutput1 = result1.getSideOutput(Descriptors.timeoutWasNotMatchedOutputTag);
        timeoutWasNotMatchedOutput1.print().name("Timeout Matched Failed1");

        printWaterMark(input);
        env.execute("Flink CEP Contiguity Conditions, Simple Pattern Example");
    }

    // 自定义事件源函数，模拟生成事件流
    public static class EventSourceFunction implements SourceFunction<Event> {

        @Override
        public void run(SourceContext<Event> ctx) throws InterruptedException {
            // 模拟事件按顺序发送
            /**
             * 2025-01-16 09:30:48 1736991048000 1736991048000L
             * 2025-01-16 09:31:48 1736991108000 1736991108000L
             * 2025-01-16 09:32:48 1736991168000 1736991168000L
             * 2025-01-16 09:33:48 1736991228000 1736991228000L
             * 2025-01-16 09:34:48 1736991288000 1736991288000L
             * 2025-01-16 09:35:48 1736991348000 1736991348000L
             */
//            ctx.collect(new Event("conn1", "a", 1736991048000L));
//            ctx.collect(new Event("conn1", "a", 1736991108000L));
//            ctx.collect(new Event("conn1", "b", 1736991168000L));
//            ctx.collect(new Event("conn1", "a", 1736991228000L));
//            ctx.collect(new Event("conn1", "a", 1736991288000L));
//            ctx.collect(new Event("conn1", "a", 1736991348000L));

            // 正常场景
//            mockNormal(ctx);
            // 模拟乱序
//            mockOutOfOrder(ctx);
            // 模拟过期
//            mockExpired(ctx);
            // 模拟多匹配模式组正常场景
            mockMultiPattenNormal(ctx);
            // 模拟多匹配模式组松散连续场景
//            mockMultiPattenNormalRelaxed(ctx);
            // 模拟多匹配模式组正常场景，其中包含多个id
//            mockMultiPattenNormal1(ctx);
            // 模拟多匹配模式组过期场景
//            mockMultiPattenExpired(ctx);
        }

        private void mockNormal(SourceContext<Event> ctx) throws InterruptedException {
            // 正常场景
            ctx.collect(new Event("conn1", "a", 1736991168000L)); //2025-01-16 09:32:48
            mockSleep();
            ctx.collect(new Event("conn1", "b", 1736991348000L)); //2025-01-16 09:35:48
        }

        private void mockOutOfOrder(SourceContext<Event> ctx) throws InterruptedException {
            ctx.collect(new Event("conn1", "b", 1736991348000L)); //2025-01-16 09:35:48
            mockSleep();
            // 模拟乱序
            ctx.collect(new Event("conn1", "a", 1736991168000L)); //2025-01-16 09:32:48
        }

        private void mockExpired(SourceContext<Event> ctx) throws InterruptedException {
            ctx.collect(new Event("conn1", "a", 1736991168000L)); //2025-01-16 09:32:48
            mockSleep();
            // 模拟过期
            ctx.collect(new Event("conn1", "k", 1736994317000L)); //2025-01-16 10:25:17
            mockSleep();

            ctx.collect(new Event("conn1", "b", 1736991348000L)); //2025-01-16 09:35:48
            mockSleep();
        }

        private void mockMultiPattenNormal(SourceContext<Event> ctx) throws InterruptedException {
            // 正常场景
            ctx.collect(new Event("conn1", "a", 1736991168000L)); //2025-01-16 09:32:48
            mockSleep();
            ctx.collect(new Event("conn1", "b", 1736991228000L)); //2025-01-16 09:33:48
            mockSleep();
            ctx.collect(new Event("conn1", "c", 1736991288000L)); //2025-01-16 09:34:48
            mockSleep();
            ctx.collect(new Event("conn1", "d", 1736991348000L)); //2025-01-16 09:35:48
            mockSleep();
            ctx.collect(new Event("conn1", "e", 1736991588000L)); //2025-01-16 09:39:48
            mockSleep();
            ctx.collect(new Event("conn1", "f", 1736991828000L)); //2025-01-16 09:43:48
        }

        private void mockMultiPattenNormalRelaxed(SourceContext<Event> ctx) throws InterruptedException {
            // 正常场景
            ctx.collect(new Event("conn1", "a", 1736991168000L)); //2025-01-16 09:32:48
            mockSleep();
            ctx.collect(new Event("conn1", "k", 1736991225000L)); //2025-01-16 09:33:48
            mockSleep();
            ctx.collect(new Event("conn1", "b", 1736991228000L)); //2025-01-16 09:33:48
            mockSleep();
            ctx.collect(new Event("conn1", "c", 1736991288000L)); //2025-01-16 09:34:48
            mockSleep();
            ctx.collect(new Event("conn1", "d", 1736991348000L)); //2025-01-16 09:35:48
            mockSleep();
            ctx.collect(new Event("conn1", "e", 1736991588000L)); //2025-01-16 09:39:48
            mockSleep();
            ctx.collect(new Event("conn1", "f", 1736991828000L)); //2025-01-16 09:43:48
        }

        private void mockMultiPattenNormal1(SourceContext<Event> ctx) throws InterruptedException {
            // 正常场景
            ctx.collect(new Event("conn1", "a", 1736991168000L)); //2025-01-16 09:32:48
            mockSleep();
            ctx.collect(new Event("conn2", "a", 1736991168000L)); //2025-01-16 09:32:48
            mockSleep();
            ctx.collect(new Event("conn1", "b", 1736991228000L)); //2025-01-16 09:33:48
            mockSleep();
            ctx.collect(new Event("conn2", "b", 1736991228000L)); //2025-01-16 09:33:48
            mockSleep();
            ctx.collect(new Event("conn1", "c", 1736991288000L)); //2025-01-16 09:34:48
            mockSleep();
            ctx.collect(new Event("conn2", "c", 1736991288000L)); //2025-01-16 09:34:48
            mockSleep();
            ctx.collect(new Event("conn1", "d", 1736991348000L)); //2025-01-16 09:35:48
            mockSleep();
            ctx.collect(new Event("conn2", "d", 1736991348000L)); //2025-01-16 09:35:48
            mockSleep();
            ctx.collect(new Event("conn1", "e", 1736991588000L)); //2025-01-16 09:39:48
            mockSleep();
            ctx.collect(new Event("conn2", "e", 1736991588000L)); //2025-01-16 09:39:48
            mockSleep();
            ctx.collect(new Event("conn1", "f", 1736991828000L)); //2025-01-16 09:43:48
            mockSleep();
            ctx.collect(new Event("conn2", "f", 1736991828000L)); //2025-01-16 09:43:48
        }

        private void mockMultiPattenExpired(SourceContext<Event> ctx) throws InterruptedException {
            ctx.collect(new Event("conn1", "a", 1736991168000L)); //2025-01-16 09:32:48
            mockSleep();
            // 模拟过期
            ctx.collect(new Event("conn1", "c", 1736994317000L)); //2025-01-16 10:25:17
            mockSleep();

            ctx.collect(new Event("conn1", "b", 1736991348000L)); //2025-01-16 09:35:48
            mockSleep();

            ctx.collect(new Event("conn1", "k", 1736995157000L)); //2025-01-16 10:39:17
            mockSleep();
        }

        private void mockSleep() throws InterruptedException {
            long sleep = 200L;
            Thread.sleep(sleep);
        }

        @Override
        public void cancel() {

        }
    }

    private static Pattern<Event, ?> createEventMatchingStrategy() {
        // ------ 创建匹配模式 "a b" ------
        Pattern<Event, ?> start = Pattern.<Event>begin("start").where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value) {
                return value.getName().startsWith("a");
            }
        });

        // 严格连续
        Pattern<Event, ?> strict = start.next("next").where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value) {
                return value.getName().startsWith("b");
            }
        }).or(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getName().startsWith("c");
            }
        });
//        .oneOrMore();

        // 松散连续
        Pattern<Event, ?> relaxed = start.followedBy("next").where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value) {
                return value.getName().startsWith("b");
            }
        });

        // 不确定松散连续
        Pattern<Event, ?> nonDRelaxed = start.followedByAny("next").where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value) {
                return value.getName().startsWith("b");
            }
        });

        return strict;
    }

    private static Pattern<Event, ?> createEventMatchingStrategy1() {
        // ------ 创建匹配模式 "d e" ------
        Pattern<Event, ?> start = Pattern.<Event>begin("start").where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value) {
                return value.getName().startsWith("c");
            }
        });

        // 严格连续
        Pattern<Event, ?> strict = start.next("next").where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value) {
                return value.getName().startsWith("d");
            }
        }).or(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getName().startsWith("e");
            }
        });
//        .oneOrMore();
        return strict;
    }

    private static Pattern<Event, ?> createEventGroupPatternMatchingStrategy() {
        // ------ 创建匹配模式 "a b" ------
        Pattern<Event, Event> pattern1 = Pattern.begin(
                Pattern.<Event>begin("start")
                        .where(new SimpleCondition<>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("a");
                            }
                        })
                        .next("next")
                        .where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("b");
                            }
                        }).next("end").where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return true;
                            }
                        })/*.where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return true;
                            }
                        })*/
                        /*.next("end").where(new IterativeCondition<Event>() {
                            @Override
                            public boolean filter(Event value, Context<Event> ctx) throws Exception {
                                Iterable<Event> eventsForPatternA = ctx.getEventsForPattern("start");
                                Iterator<Event> iteratorA = eventsForPatternA.iterator();
                                String cd_a = "循环匹配a: ";
                                while (iteratorA.hasNext()) {
                                    cd_a += iteratorA.next();
                                }
                                System.out.println(cd_a);

                                Iterable<Event> eventsForPatternB = ctx.getEventsForPattern("next");
                                Iterator<Event> iteratorB = eventsForPatternB.iterator();
                                String cd_b = "循环匹配b: ";
                                while (iteratorB.hasNext()) {
                                    cd_b += iteratorB.next();
                                }
                                System.out.println(cd_b);
                                return true;
                            }
                        })*/
        ).within(Time.minutes(5));

        Pattern<Event, Event> pattern2 = Pattern.begin(
                Pattern.<Event>begin("start2")
                        .where(new SimpleCondition<>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("c");
                            }
                        })
                        .next("next2")
                        .where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("d");
                            }
                        }).next("end2")/*.where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return true;
                            }
                        })*/
        ).within(Time.minutes(5));

        Pattern<Event, Event> pattern3 = Pattern.begin(
                Pattern.<Event>begin("start3")
                        .where(new SimpleCondition<>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("e");
                            }
                        })
                        .next("next3")
                        .where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("f");
                            }
                        })/*.next("end3")*/
        ).within(Time.minutes(5));

        Pattern<Event, Event> patternGroup =
                Pattern.begin(pattern1).optional()
                        .next(pattern2).optional()
                        .next(pattern3).optional();
        return patternGroup;
    }

    private static Pattern<Event, ?> createEventGroupPatternMatchingStrategyInProcessingTime() {
        // ------ 创建匹配模式 "a b" ------
        Pattern<Event, Event> pattern1 = Pattern.begin(
                Pattern.<Event>begin("start")
                        .where(new SimpleCondition<>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("a");
                            }
                        })
                        .next("next")
                        .where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("b");
                            }
                        }).next("end").where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return true;
                            }
                        })
        );

        Pattern<Event, Event> pattern2 = Pattern.begin(
                Pattern.<Event>begin("start2")
                        .where(new SimpleCondition<>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("c");
                            }
                        })
                        .next("next2")
                        .where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("d");
                            }
                        }).next("end2")
        );

        Pattern<Event, Event> pattern3 = Pattern.begin(
                Pattern.<Event>begin("start3")
                        .where(new SimpleCondition<>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("e");
                            }
                        })
                        .next("next3")
                        .where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().startsWith("f");
                            }
                        })
        );

        Pattern<Event, Event> patternGroup =
                Pattern.begin(pattern1).optional()
                        .next(pattern2).optional()
                        .next(pattern3).optional();
        return patternGroup;
    }

    private static void printWaterMark(DataStream<Event> input) {
        input.process(new ProcessFunction<Event, Long>() {

            @Override
            public void processElement(Event value, ProcessFunction<Event, Long>.Context ctx, Collector<Long> out) throws Exception {
                long currentWatermark = ctx.timerService().currentWatermark();
                String formatted = DateUtil.format(new Date(currentWatermark), DatePattern.NORM_DATETIME_MS_PATTERN);
                System.out.println("currentWatermark:" + formatted);
            }
        });
    }

    static class MyPatternProcessFunction extends PatternProcessFunction<Event, String> implements TimedOutPartialMatchHandler<Event> {

        @Override
        public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) throws Exception {
//            System.out.println("2.打印调用堆栈");
//            Stream.of(Thread.currentThread().getStackTrace()).forEach(System.out::println);
            String strResult = "Matched Successes: ";
            // Check if sth equals null so that the optional() quantifier can be used
            if (match.get("start") != null) {

                for (int i = 0; i < match.get("start").size(); i++) { // for looping patterns
                    strResult += match.get("start").get(i).getId() + "->" + match.get("start").get(i).getName() + " ";
                }
            }

            if (match.get("next") != null) {
                for (int i = 0; i < match.get("next").size(); i++) {
                    strResult += match.get("next").get(i).getId() + "->" + match.get("next").get(i).getName() + " ";
                }
            }

            if (match.get("start2") != null) {
                for (int i = 0; i < match.get("start2").size(); i++) { // for looping patterns
                    strResult += match.get("start2").get(i).getId() + "->" + match.get("start2").get(i).getName() + " ";
                }
            }
            if (match.get("next2") != null) {
                for (int i = 0; i < match.get("next2").size(); i++) {
                    strResult += match.get("next2").get(i).getId() + "->" + match.get("next2").get(i).getName() + " ";
                }
            }

            if (match.get("start3") != null) {
                for (int i = 0; i < match.get("start3").size(); i++) { // for looping patterns
                    strResult += match.get("start3").get(i).getId() + "->" + match.get("start3").get(i).getName() + " ";
                }
            }
            if (match.get("next3") != null) {
                for (int i = 0; i < match.get("next3").size(); i++) {
                    strResult += match.get("next3").get(i).getId() + "->" + match.get("next3").get(i).getName() + " ";
                }
            }
            out.collect(strResult);
        }

        @Override
        public void processTimedOutMatch(Map<String, List<Event>> match, Context ctx) throws Exception {
            String strResult = "Timeout Matched Failed: ";
            // Check if sth equals null so that the optional() quantifier can be used
            if (match.get("start") != null) {
                for (int i = 0; i < match.get("start").size(); i++) { // for looping patterns
                    strResult += match.get("start").get(i).getName() + "->" + match.get("start").get(i).getName() + " ";
                }
            }
            if (match.get("next") != null) {
                for (int i = 0; i < match.get("next").size(); i++) {
                    strResult += match.get("next").get(i).getName() + "->" + match.get("next").get(i).getName() + " ";
                }
            }

            if (match.get("start2") != null) {
                for (int i = 0; i < match.get("start2").size(); i++) { // for looping patterns
                    strResult += match.get("start2").get(i).getName() + "->" + match.get("start2").get(i).getName() + " ";
                }
            }
            if (match.get("next2") != null) {
                for (int i = 0; i < match.get("next2").size(); i++) {
                    strResult += match.get("next2").get(i).getName() + "->" + match.get("next2").get(i).getName() + " ";
                }
            }

            if (match.get("start3") != null) {
                for (int i = 0; i < match.get("start3").size(); i++) { // for looping patterns
                    strResult += match.get("start3").get(i).getName() + "->" + match.get("start3").get(i).getName() + " ";
                }
            }
            if (match.get("next3") != null) {
                for (int i = 0; i < match.get("next3").size(); i++) {
                    strResult += match.get("next3").get(i).getName() + "->" + match.get("next3").get(i).getName() + " ";
                }
            }
            ctx.output(Descriptors.timeoutWasNotMatchedOutputTag, strResult);
        }
    }


    public static class Descriptors {
        public static final OutputTag<String> timeoutWasNotMatchedOutputTag = new OutputTag<String>("timeoutWasNotMatched") {
        };
    }

    public static class MySerializableTimestampAssigner implements SerializableTimestampAssigner<Event> {
        @Override
        public long extractTimestamp(Event event, long timestamp) {
            long eventTimestamp = event.getTimestamp();
//            String formatted = DateUtil.format(new Date(eventTimestamp), DatePattern.NORM_DATETIME_MS_PATTERN);
//            System.out.println("eventTimestamp:" + formatted);
            return eventTimestamp;
        }
    }

    public static void ctiCepCaseNPattern1Stream() throws Exception {
        // 创建 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);
        env.setParallelism(1);

        DataStream<Event> input = env.addSource(new EventSourceFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Event>forMonotonousTimestamps() // 对事件使用单调递增的时间戳
                        .<Event>forBoundedOutOfOrderness(Duration.ofMinutes(3)) // 允许乱序
                        .withTimestampAssigner(new MySerializableTimestampAssigner()));

        // 设置可选的分区
        KeyedStream<Event, String> eventKeyedStream = input.keyBy(Event::getId);

        // 设置事件匹配策略
        Pattern<Event, ?> eventMatchingStrategy = createEventGroupPatternMatchingStrategy();

        // 设置最大有效时间间隔
        Pattern<Event, ?> pattern = eventMatchingStrategy/*.within(Time.minutes(5))*/;

        // 使用 CEP 识别模式，返回 PatternStream 类型
        PatternStream<Event> patternStream = CEP.pattern(eventKeyedStream, pattern);

        // 处理匹配和未匹配的数据
        SingleOutputStreamOperator<String> result = patternStream.process(new MyPatternProcessFunction());

        // 打印匹配成功项
        result.print().name("Matched Successes");

        // 使用侧输出流，打印匹配失败项
        SideOutputDataStream<String> timeoutWasNotMatchedOutput = result.getSideOutput(Descriptors.timeoutWasNotMatchedOutputTag);
        timeoutWasNotMatchedOutput.print().name("Timeout Matched Failed");

//        printWaterMark(input);
        env.execute("Flink CEP Contiguity Conditions, Simple Pattern Example");
    }

    public static void ctiCepCasePatternGroupInProcessingTime() throws Exception {
        // 创建 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> input = env.addSource(new EventSourceFunction());

        // 设置可选的分区
        KeyedStream<Event, String> eventKeyedStream = input.keyBy(Event::getId);

        // 设置事件匹配策略
        Pattern<Event, ?> eventMatchingStrategy = createEventGroupPatternMatchingStrategyInProcessingTime();

        // 设置最大有效时间间隔
        Pattern<Event, ?> pattern = eventMatchingStrategy;

        // 使用 CEP 识别模式，返回 PatternStream 类型
        PatternStream<Event> patternStream = CEP.pattern(eventKeyedStream, pattern).inProcessingTime();

        // 处理匹配和未匹配的数据
        SingleOutputStreamOperator<String> result = patternStream.process(new MyPatternProcessFunction());

        // 打印匹配成功项
        result.print().name("Matched Successes");

        // 使用侧输出流，打印匹配失败项
        SideOutputDataStream<String> timeoutWasNotMatchedOutput = result.getSideOutput(Descriptors.timeoutWasNotMatchedOutputTag);
        timeoutWasNotMatchedOutput.print().name("Timeout Matched Failed");

//        printWaterMark(input);
        env.execute("Flink CEP Contiguity Conditions, Simple Pattern Example");
    }
}
