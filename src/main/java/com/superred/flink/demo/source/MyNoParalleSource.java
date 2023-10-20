package com.superred.flink.demo.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 *
 */
public class MyNoParalleSource implements SourceFunction<Long> {

    private long count = 1L;

    private boolean isRunning = true;


    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    /**
     * 取消一个cancel的时候会调用的方法
     */
    @Override
    public void cancel() {
        isRunning = false;
    }

}
