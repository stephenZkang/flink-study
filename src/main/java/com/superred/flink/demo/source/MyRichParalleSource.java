package com.superred.flink.demo.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义实现一个支持并行度的source
 *
 * RichParallelSourceFunction 会额外提供open和close方法
 * 针对source中如果需要获取其他链接资源，那么可以在open方法中获取资源链接，在close中关闭资源链接
 *
 */
public class MyRichParalleSource extends RichParallelSourceFunction<String> {

    private long count = 1L;

    private boolean isRunning = true;


    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning){
            ctx.collect(Long.toString(count)+","+Long.toString(count));
            count++;
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }

    /**
     * 这个方法只会在最开始的时候被调用一次
     * 实现获取链接的代码
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open.............");
        super.open(parameters);
    }

    /**
     * 实现关闭链接的代码
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }
}
