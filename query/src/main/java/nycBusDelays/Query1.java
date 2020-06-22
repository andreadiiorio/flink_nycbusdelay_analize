package nycBusDelays;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.file.FileSystems;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Query1 {
    private static final String CSV_LIST_SEP = " ";
    public static String HOSTNAME = "localhost";//"172.17.0.1";
    public static int PORT = 5555;
    public static Time WINDOW_SIZE = Time.days(7);
    private static int SINK_PARALLELISM = 1;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get input data by connecting to the socket
        DataStream<String> lines = env.socketTextStream(HOSTNAME, PORT, "\n", 1);

        //parse and extract timestamp into tuple of <timeStamp,boro,delayRecorded>
        DataStream<Tuple3<Long, String, Float>> delaysNeighboro = lines.map(new MapFunction<String, Tuple3<Long, String, Float>>() {
            @Override
            public Tuple3<Long, String, Float> map(String line) throws Exception {

                String[] fields = line.split("\\s");                            //"occurredOn","boro","howLongDelayed"return null;
                return new Tuple3(Long.valueOf(fields[0]), fields[1], Float.valueOf(fields[2]));
            } //extract timestamp from first field
        }).assignTimestampsAndWatermarks(new WatermarkingStrict()); //eventTimeWatermarking //TODO RETRY WITH ASCENDING TS -> EXPECTED EURISTIC WATERMARKING -> FLUCTUATING RESOULTS

        ///aggregate delays per boro in averages from EventTime Windows -> out: <startTS,"boro avg(of boro in [startTS,startTS+winSize)" )
        DataStream<Tuple2<Long, String>> delaysNeighboroAverges = delaysNeighboro.keyBy(1).timeWindow(WINDOW_SIZE)
                .aggregate(new AverageAggregate(),new ProcessWindowFunction<Double, Tuple2<Long, String>, Tuple, TimeWindow>() {
                    //combine the average with window start times
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Double> elements, Collector<Tuple2<Long, String>> out) throws Exception {
                        String boroKey = tuple.getField(0);
                        Iterator<Double> all = elements.iterator();
                        Double avgOfBoro = all.next();//elements.iterator().next();    //expected only 1 element
                        if (all.hasNext())  System.err.println("NOT 1! AGGREGATED ELEMENT" + all.next());    //TODO DEBUG CHECK
                        Long startTimeOfWindow = context.window().getStart();
                        out.collect(new Tuple2<>(startTimeOfWindow, boroKey + CSV_LIST_SEP + avgOfBoro + CSV_LIST_SEP));
                    }
                });

        //group output neighboro's averages by starting timeStamp
        //reduce by rewindowing
        DataStream<Tuple2<Long, String>> out = delaysNeighboroAverges.keyBy(0)
                .timeWindow(WINDOW_SIZE).reduce(new ReduceFunction<Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> reduce(Tuple2<Long, String> value1, Tuple2<Long, String> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + CSV_LIST_SEP + value2.f1 + CSV_LIST_SEP);
            }
        });
        /*.reduce(new ReduceFunction<Tuple2<Long, String>>() {      //TODO CHECK NO RE WINDOWING
            @Override
            public Tuple2<Long, String> reduce(Tuple2<Long, String> value1, Tuple2<Long, String> value2) throws Exception {
                if (! value1.f0.equals(value2.f0)) System.err.println("FKK KEYS" + value1.f0 + " " + value2.f0); //TODO DEBUG CHECK
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });
//        SingleOutputStreamOperator<Tuple2<Long, String>> outt = delaysNeighboroAverges.keyBy(0).sum(0);
*/


        Path outFile=Path.fromLocalFile(new File("/home/andysnake/IdeaProjects/sabdPrj2/1.csv"));
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(outFile,new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        out.map(new MapFunction<Tuple2<Long, String>, String>() {
            @Override
            public String map(Tuple2<Long, String> value) throws Exception {
                return Utils.convertTs(value.f0,false)+CSV_LIST_SEP+value.f1;
            }
        }).addSink(sink).setParallelism(1);
        env.execute("Q1");
    }

    ;

    /**
     * Strict watermarking generation -> a watermark produced after each encountered element
     */
    private static class WatermarkingStrict implements AssignerWithPunctuatedWatermarks<Tuple3<Long, String, Float>> {

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple3<Long, String, Float> lastElement, long extractedTimestamp) {
            //TODO WATERMARK ONLY AFTER TIME DELAY (WIN SIZE) passed from last element time stamp <- from windowSize timeStamp conversion
            //e.g. if lastElement > before +winSize -> watermark...
            return new Watermark(lastElement.f0);
        }

        @Override
        public long extractTimestamp(Tuple3<Long, String, Float> element, long previousElementTimestamp) {
            return element.f0;
        }
    }
}
