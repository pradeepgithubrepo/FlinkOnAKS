package com.example.app;//Copyright (c) Microsoft Corporation. All rights reserved.
//Licensed under the MIT License.
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;     //v0.11.0.0
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;

public class FlinkTestProducer {

    private static final String TOPIC = "pradeh";
//    private static final String FILE_PATH = "src/main/resources/producer.config";

    public static void main(String... args) {
        try {
            Properties properties = new Properties();
//            properties.load(new FileReader(FILE_PATH));
            properties.put("bootstrap.servers","pradeepehname.servicebus.windows.net:9093");
            properties.put("client.id","FlinkExampleProducer");
            properties.put("sasl.mechanism","PLAIN");
            properties.put("security.protocol","SASL_SSL");
            properties.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://pradeepehname.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";");

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream stream = createStream(env);

            FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                    TOPIC,    
                    new SimpleStringSchema(),   // serialization schema
                    properties);

            stream.addSink(myProducer);
            env.execute("Testing flink print");

        } catch(FileNotFoundException e){
            System.out.println("FileNotFoundException: " + e);
        } catch (Exception e) {
            System.out.println("Failed with exception:: " + e);
        }
    }

    public static DataStream createStream(StreamExecutionEnvironment env){
        return env.generateSequence(0, 200)
            .map(new MapFunction<Long, String>() {
                @Override
                public String map(Long in) {
                    return "FLINK PRODUCE " + in;
                }
            });
    }
}
