package com.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketStreamingJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> textStream = env.socketTextStream("localhost", 9999, "\n");
        DataStream<String> mappedStream = textStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        });
        DataStream<String> filteredStream = mappedStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.length() > 3;
            }
        });
        filteredStream.print();
        env.execute("Socket Streaming Job");
    }
}
