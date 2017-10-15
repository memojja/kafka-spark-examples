package streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import java.util.*;
import java.util.logging.Logger;



import java.util.*;

public class StreamingDemo {

    public static void main(String[] args) throws InterruptedException {


        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton("first_topic");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(
                ssc,String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics
        );

        List<String> datas = new ArrayList<>();

        directKafkaStream.foreachRDD(rdd -> {
            System.out.println(rdd.partitions().size() + rdd.count());
            rdd.foreach(record -> {
                datas.add(record._2 + "*******");
                datas.stream().map(data -> "---"+data+"===" )
                        .forEach(System.out::println);
                System.out.println(record._2 + "*******");
            });
        });

        datas.stream().map(data -> "---"+data+"===" )
                .forEach(System.out::println);
        System.out.println("-----");

        directKafkaStream.print();
        ssc.start();
        ssc.awaitTermination();


    }
}
