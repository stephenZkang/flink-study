package com.superred.flink.demo.source.window;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class WindowSource implements SourceFunction<String> {


    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        String[] datas = {
                "a,1575159390000",
                "a,1575159402000",
                "b,1575159427000",
                "c,1575159382000",
                "b,1575159407000",
                "a,1575159302000"
        };

        for (int k = 0; k < datas.length; k++) {
            Thread.sleep(100);
            ctx.collect(datas[k]);
        }
    }

    @Override
    public void cancel() {

    }
}
