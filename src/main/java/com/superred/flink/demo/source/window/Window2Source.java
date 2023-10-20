package com.superred.flink.demo.source.window;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Window2Source implements SourceFunction<String> {


    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        String[] datas = {
                "a,1575159381300",
                "a,1575159399000",
                "d,1575159397000",
                "f,1575159384000"
        };
        for (int k = 0; k < datas.length; k++) {
            ctx.collect(datas[k]);
        }
    }

    @Override
    public void cancel() {

    }
}
