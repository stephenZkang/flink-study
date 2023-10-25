package com.superred.flink.demo;

import com.superred.flink.demo.model.Student;
import com.superred.flink.demo.partition.MyPartition;
import com.superred.flink.demo.sink.SinkToMySQL;
import com.superred.flink.demo.source.MyNoParalleSource;
import com.superred.flink.demo.source.MyParalleSource;
import com.superred.flink.demo.source.MyRichParalleSource;
import com.superred.flink.demo.source.window.Window2Source;
import com.superred.flink.demo.source.window.WindowSource;
import com.superred.flink.demo.utils.ExecutionEnvUtil;
import com.superred.flink.demo.utils.GsonUtil;
import com.superred.flink.demo.utils.KafkaConfigUtil;
import com.superred.flink.demo.utils.KafkaTopic;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Main  {

    public static void main(String[] args) throws Exception {
//        flinkKafkaToMysql();
//        noParallelismSource();
//        sinkSource();
//        joinWinSource();
//        innerJoinSource();
//        streamToRedis();
//        streamingDemoWithMyParitition();
//        streamingConnect();
//        streamingFilter();
//        streamingUnion();
//        streamingBroadcast();
//        streamingWindowWatermark();

    }

    /**
     * 水印解释的非常到位，请看下面分享
     *  https://www.cnblogs.com/rossiXYZ/p/12286407.html
     * @throws Exception
     */
    public static void streamingWindowWatermark() throws Exception {
        //定义socket的端口号
        int port = 9000;
        String ip = "10.66.77.35";
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置使用eventtime，默认是使用processtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //设置并行度为1,默认并行度是当前机器的cpu数量
        env.setParallelism(1);

        //连接socket获取输入的数据
        DataStream<String> text = env.socketTextStream(ip, port, "\n");

        //解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        //抽取timestamp和生成watermark
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 10000L;// 最大允许的乱序时间是10s

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            /**
             * 定义生成watermark的逻辑
             * 默认100ms被调用一次
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            //定义如何提取timestamp
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                long id = Thread.currentThread().getId();
                System.out.println("currentThreadId:"+id+",key:"+element.f0+",eventtime:["+element.f1+"|"+sdf.format(element.f1)+"],currentMaxTimestamp:["+currentMaxTimestamp+"|"+
                        sdf.format(currentMaxTimestamp)+"],watermark:["+getCurrentWatermark().getTimestamp()+"|"+sdf.format(getCurrentWatermark().getTimestamp())+"]");
                return timestamp;
            }
        });

        DataStream<String> window = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))//按照消息的EventTime分配窗口，和调用TimeWindow效果一样
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    /**
                     * 对window内的数据进行排序，保证数据的顺序
                     * @param tuple
                     * @param window
                     * @param input
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrarList = new ArrayList<Long>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrarList.add(next.f1);
                        }
                        Collections.sort(arrarList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = key + "," + arrarList.size() + "," + sdf.format(arrarList.get(0)) + "," + sdf.format(arrarList.get(arrarList.size() - 1))
                                + "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
                        out.collect(result);
                    }
                });
        //测试-把结果打印到控制台即可
        window.print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("eventtime-watermark");

    }

    /**
     *  Broadcast
     *  https://blog.csdn.net/allensandy/article/details/106221397
     * @throws Exception
     */
    public static void streamingBroadcast() throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //获取数据源
        DataStreamSource<Long> text = env
                .addSource(new MyNoParalleSource()).setParallelism(1);//注意：针对此source，并行度只能设置为1

        DataStream<Long> num = text
                .broadcast()
                .map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                long id = Thread.currentThread().getId();
                System.out.println("线程id："+id+",接收到数据：" + value);
                return value;
            }
        });

        //每2秒钟处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        //打印结果
        sum.print().setParallelism(1);

        env.execute("streamingBroadcast");

    }

    /**
     *  Filter
     * @throws Exception
     */
    public static void streamingUnion() throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //设置并行度
        DataStreamSource<Long> text1 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        DataStreamSource<Long> text2 = env.addSource(new MyNoParalleSource()).setParallelism(1);


        DataStream<Long> union = text1.union(text2);


        SingleOutputStreamOperator<Long> num = union.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始接收到数据：" + value);
                return value;
            }
        });

        //每2秒钟处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        //打印结果
        sum.print().setParallelism(1);

        env.execute("streamingUnion");

    }


    /**
     *  Filter
     * @throws Exception
     */
    public static void streamingFilter() throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<Long> text1 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<Long> num = text1.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始接收到数据：" + value);
                return value;
            }
        });

        SingleOutputStreamOperator<Long> filterData = num.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });

        SingleOutputStreamOperator<Long> resultData = filterData.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("过滤之后接收到数据：" + value);
                return value;
            }
        });

        SingleOutputStreamOperator<Long> sum = resultData.timeWindowAll(Time.seconds(2)).sum(0);

        //打印结果
        sum.print().setParallelism(1);

        env.execute("streamingFilter");

    }


    /**
     * connect
     *   和union类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法
     *
     * @throws Exception
     */
    public static void streamingConnect() throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text1 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        DataStreamSource<Long> text2 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<String> text2_str = text2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str_" + value;
            }
        });

        ConnectedStreams<Long, String> connectStream = text1.connect(text2_str);


        SingleOutputStreamOperator<Object> result = connectStream.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long value) throws Exception {
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        });


        //打印结果
        result.print().setParallelism(1);

        env.execute("streamingConnect");

    }

    /**
     * 分区
     *      奇数分区 1
     *      偶数分区 2
     *  分区数 > 并行度
     * @throws Exception
     */
    public static void streamingDemoWithMyParitition() throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource());

        //对数据进行转换，把long类型转成tuple1类型
        DataStream<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });

        //分区之后的数据
        DataStream<Tuple1<Long>> partitionData= tupleData
                .partitionCustom(new MyPartition(), 0);

        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程id：" + Thread.currentThread().getId() + ",value: " + value);
                return value.getField(0);
            }
        });

        result.print().setParallelism(1);

        env.execute("SteamingDemoWithMyParitition");

    }


    /**
     *
     *  10.66.77.35 服务器上shell执行
     *      nc -lvnp 9000
     *  10.66.77.35 服务器上shell执行
     *      redis-cli -h 10.66.77.35 -p 6379
     *      auth test123
     *      lrange l_words 0 -1
     *
     * @throws Exception
     */
    public static void streamToRedis() throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env
                .socketTextStream("10.66.77.35", 9000, "\n");

        DataStream<Tuple2<String, String>> l_word_data = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>("l_words",s);
            }
        });

        //创建redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig
                .Builder().setHost("10.66.77.35").setPort(6379).setPassword("test123").build();

        //创建redissink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());

        l_word_data.addSink(redisSink);

        env.execute("StreamingDemoToRedis");

    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {
        //表示从接收的数据中获取需要操作的redis key
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }
        //表示从接收的数据中获取需要操作的redis value
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }
    }

    /**
     * 滚动窗口关联数据操作是将滚动窗口中相同的Key的两个Datastream数据集中的元素进行关联，
     *          并应用用户自定义的JoinFunction计算关联结果。
     *
     * @throws Exception
     */
    public static void innerJoinSource() throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.生成dataStream1，interval join之前必须要生成WM，即实现assignTimestampsAndWatermarks方法
        DataStream<Tuple2<String,Long>> dataStream1 = env
                .addSource(new WindowSource()).map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] words = s.split(",");
                        return new Tuple2<>(words[0] , Long.parseLong(words[1]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofMinutes(1L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String,Long> input, long l) {
                                return input.f1;
                            }
                        }));

        // 3.生成dataStream2，interval join之前必须要生成WM，即实现assignTimestampsAndWatermarks方法
        DataStream<Tuple2<String,Long>> dataStream2 = env.addSource(new Window2Source()).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] words = s.split(",");
                return new Tuple2<>(words[0] , Long.parseLong(words[1]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofMinutes(1L))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String,Long> input, long l) {
                        return input.f1;
                    }
                }));


        // interval join
        dataStream1.keyBy(key -> key.f0)
                .intervalJoin(dataStream2.keyBy(key -> key.f0))
                .between(Time.seconds(0L),Time.seconds(2L))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, Object>() {
                    @Override
                    public void processElement(Tuple2<String, Long> t1,
                                               Tuple2<String, Long> t2,
                                               Context context, Collector<Object> collector) throws Exception {
                        collector.collect(new Tuple4<>(t1.f0,t1.f1,t2.f0,t2.f1));
                    }
                }).print();

        env.execute("inner-join-source");

    }

    /**
     * 滚动窗口关联数据操作是将滚动窗口中相同的Key的两个Datastream数据集中的元素进行关联，
     *          并应用用户自定义的JoinFunction计算关联结果。
     *
     * @throws Exception
     */
    public static void joinWinSource() throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //获取数据源
        DataStream<Tuple2<String, Long>> dataStream1 = env
                .addSource(new WindowSource()).map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] words = s.split(",");
                        return new Tuple2<>(words[0], Long.parseLong(words[1]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofMinutes(1L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> input, long l) {
                                return input.f1;
                            }
                        }));

        DataStream<Tuple2<String, Long>> dataStream2 = env
                .addSource(new Window2Source()).map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] words = s.split(",");
                        return new Tuple2<>(words[0], Long.parseLong(words[1]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofMinutes(1L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> input, long l) {
                                return input.f1;
                            }
                        }));


        dataStream1.join(dataStream2)
                .where(key -> key.f0)
                .equalTo(key -> key.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1L)))
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, Object>() {
                    @Override
                    public Object join(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return new Tuple4<>(t1.f0,t1.f1,t2.f0,t2.f1);
                    }
                })
                .print();

        env.execute("join-win-source");
    }

    /**
     *
     * @throws Exception
     */
    private static void sinkSource() throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //获取数据源
        DataStreamSource<String> text = env
                .addSource(new MyRichParalleSource()).setParallelism(2);//注意：针对此source，并行度只能设置为1

        SingleOutputStreamOperator<Tuple2<Long, Long>> num = text.map(new MapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(String value) throws Exception {
//                System.out.println("接收到数据：" + value);
                String[] split = value.split(",");
                System.out.println("接收到数据：" + Long.valueOf(split[0])+","+(Long.valueOf(split[1])+1L));
                return new Tuple2<>(Long.valueOf(split[0]), Long.valueOf(split[1])+1L);
            }
        });

        //每2秒钟处理一次数据
        SingleOutputStreamOperator<Tuple2<Long, Long>> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        //打印结果
        sum.print().setParallelism(1);

        env.execute("sink-source");
    }



    /**
     * 错误信息
     * Record has Long.MIN_VALUE timestamp (= no timestamp marker).
     *      Is the time characteristic set to 'ProcessingTime',
     *      or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?
     * 解决方法: https://blog.csdn.net/langlang1111111/article/details/121343530
     *
     * @throws Exception
     */
    private static void noParallelismSource() throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 错误信息
         *      * Record has Long.MIN_VALUE timestamp (= no timestamp marker).
         *      *      Is the time characteristic set to 'ProcessingTime',
         *      *      or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //获取数据源
        DataStreamSource<String> text = env
                .addSource(new MyRichParalleSource()).setParallelism(2);//注意：针对此source，并行度只能设置为1

        DataStream<Long> num = text.map(new MapFunction<String, Long>() {
            @Override
            public Long map(String value) throws Exception {
                System.out.println("接收到数据：" + value);
                return Long.parseLong(value.split(",")[0]);
            }
        });

        //每2秒钟处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        //打印结果
        sum.print().setParallelism(1);

        env.execute("no-parallelism-source");
    }

    private static void flinkKafkaToMysql() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);
        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<>(
                KafkaTopic.STUDENT_TOPIC.getTopic(),
                new SimpleStringSchema(),
                properties
        )).setParallelism(1).map(s -> GsonUtil.fromJson(s, Student.class));

        student.addSink(new SinkToMySQL());
        env.execute("flink-kafka-to-mysql");
    }
}
