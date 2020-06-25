package nycBusDelays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Tuple3;

import javax.xml.crypto.Data;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
public class Query1SparkStreaming {
    public static void main(String[] args) throws InterruptedException, StreamingQueryException {// Create a local StreamingContext with two working thread and batch interval of 1 second

//        jssc.sparkContext().setLogLevel("ERROR");
//        // Create a DStream that will connect to hostname:port, like localhost:9999
//        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999, StorageLevels.MEMORY_ONLY);
//        // Split each line into words
//        JavaDStream<Tuple3<Long, String, String>> delays = lines.map(line -> {
//            String[] fields = line.split(" ");
//            return new Tuple3<>(Long.valueOf(fields[0]), fields[1], fields[2]);
//        });
        SparkSession spark = SparkSession
                .builder().master("local[*]")
                .appName("JavaStructuredNetworkWordCountWindowed")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        // Create DataFrame representing the stream of input lines from connection to host:port
        StructType schema=new StructType().add("timestamp","TimeStamp").add("boro","String").add("delay","Float");
        Dataset<Row> socketSrc= spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 5555)
                .load();
        // Split the lines into words, retaining timestamps
        Dataset<String> lines=socketSrc.as(Encoders.STRING());
        lines.printSchema();
        Dataset<Row> fields = lines.map(new MapFunction<String, Tuple3<Timestamp, String, Float>>() {
            @Override
            public Tuple3<Timestamp, String, Float> call(String s) throws Exception {
                String[] parts = s.split(" ");
                return new Tuple3<>(new Timestamp(Long.parseLong(parts[0])),parts[1],Float.parseFloat(parts[2]));
            }},Encoders.tuple(Encoders.TIMESTAMP(),Encoders.STRING(),Encoders.FLOAT()))
                .toDF("timestamp","boro","delay").withWatermark("timestamp","1 minutes");
        fields.printSchema();
        // Group the data by window and word and compute the count of each group
        Dataset<Row> windowedCounts = fields.withWatermark("timestamp","1 minutes").groupBy(
                functions.window(fields.col("timestamp"), "24 minutes","24 minutes")
        ).count();
        windowedCounts.printSchema();
        // Start running the query that prints the windowed word counts to the console
        StreamingQuery query = windowedCounts.writeStream()
                .format("console")
                .option("truncate", "false")
                .outputMode("Append")
                .start();

        query.awaitTermination();
    }
}
