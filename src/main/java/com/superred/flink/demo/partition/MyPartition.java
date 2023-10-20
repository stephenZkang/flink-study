package com.superred.flink.demo.partition;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartition implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区总数："+numPartitions);
        if(key % 3 == 0){
            return 0;
        }else if(key % 3 == 1){
            return 1;
        }else{
            return 2;
        }
    }
}