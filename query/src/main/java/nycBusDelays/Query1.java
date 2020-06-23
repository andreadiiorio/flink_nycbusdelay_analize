package nycBusDelays;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Iterator;


/**
 *
 */
public class Query1 {
    private static final String CSV_LIST_SEP = " ";
    private static final String OUT_PATH = "csv1";
    public static String HOSTNAME = "localhost";//"172.17.0.1";
    public static int PORT = 5555;
    private static final String CSV_SEP = ",";
    public static Time WINDOW_SIZE = Time.hours(24);
    private static int SINK_PARALLELISM = 1;

    public static void main(String[] args) throws Exception {
        long end,start=System.nanoTime();
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
        }).assignTimestampsAndWatermarks(new WatermarkingStrict());

        ///aggregate delays per boro in averages from EventTime Windows -> out: <startTS,"boro avg(of boro in [startTS,startTS+winSize)" )
        DataStream<Tuple2<Long, String>> delaysNeighboroAverges = delaysNeighboro.keyBy(1).timeWindow(WINDOW_SIZE)
                .aggregate(new AverageAggregate(),new ProcessWindowFunction<Double, Tuple2<Long, String>, Tuple, TimeWindow>() {
                    //combine the average with window start times
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Double> elements, Collector<Tuple2<Long, String>> out) throws Exception {
                        String boroKey = tuple.getField(0);
                        Iterator<Double> all = elements.iterator();
                        Double avgOfBoro = all.next();//elements.iterator().next();    //expected only 1 element
                        Long startTimeOfWindow = context.window().getStart();
                        out.collect(new Tuple2<>(startTimeOfWindow, boroKey + CSV_LIST_SEP + avgOfBoro + CSV_LIST_SEP));
                    }
                });
        //TODO FLINK WINDOWS START TIME FKK!! delaysNeighboroAverges.map(new ConvertTs()).print().setParallelism(1);
        //group output neighboro's averages by starting timeStamp
        //reduce by rewindowing
        DataStream<Tuple2<Long, String>> out = delaysNeighboroAverges.keyBy(0)
                .timeWindow(WINDOW_SIZE).reduce(new ReduceFunction<Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> reduce(Tuple2<Long, String> value1, Tuple2<Long, String> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + CSV_LIST_SEP + value2.f1 + CSV_LIST_SEP);
                    }
                });
        out.map(new ConvertTs()).addSink(Utils.fileOutputSink(OUT_PATH)).setParallelism(SINK_PARALLELISM);
        env.execute("Q1");
        end=System.nanoTime();
        System.out.println("elapsed: "+((double)(end-start))/1000000000);
    }


    private static class ConvertTs implements MapFunction<Tuple2<Long, String>, String> {
        @Override
        public String map(Tuple2<Long, String> value) {
            return Utils.convertTs(value.f0,true)+CSV_SEP+value.f1;
        }
    }

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
