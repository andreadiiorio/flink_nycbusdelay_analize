package nycBusDelays;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Properties;

public class Query1 {

    public static void main(String[] args) throws Exception {
        //init vars
        final Properties prop = Utils.getUtils().getProperty();
        final String CSV_SEP = prop.getProperty("CSV_SEP");
        Time WINDOW_SIZE;
        if (prop.containsKey("WIN_HOURS")) WINDOW_SIZE = Time.hours(Integer.parseInt(prop.getProperty("WIN_HOURS")));
        else WINDOW_SIZE = Time.days(Integer.parseInt(prop.getProperty("WIN_DAYS")));
        boolean FineTuneParallelism=false;
        if (Boolean.parseBoolean(prop.getProperty("FineTuneParallelism")))  FineTuneParallelism=true;

        long end,start=System.nanoTime();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get input data by connecting to the socket
        DataStream<String> lines = env.socketTextStream(prop.getProperty("HOSTNAME"), Integer.parseInt(prop.getProperty("PORT")), "\n", 1);

        //parse and extract timestamp into tuple of <timeStamp,boro,delayRecorded>
        SingleOutputStreamOperator <Tuple3<Long, String, Float>> delaysNeighboro = lines.map(new MapFunction<String, Tuple3<Long, String, Float>>() {
            @Override
            public Tuple3<Long, String, Float> map(String line) throws Exception {

                String[] fields = line.split("\\s");                            //"occurredOn","boro","howLongDelayed"return null;
                return new Tuple3(Long.valueOf(fields[0]), fields[1], Float.valueOf(fields[2]));
            } //extract timestamp from first field
        });
        if (FineTuneParallelism) delaysNeighboro.setParallelism(Integer.parseInt(prop.getProperty("q1_map1_parallelism")));
        delaysNeighboro= delaysNeighboro.assignTimestampsAndWatermarks(new AscendingWatermarking());
        if (FineTuneParallelism) delaysNeighboro.setParallelism(Integer.parseInt(prop.getProperty("q1_map1_parallelism")));

        ///aggregate delays per boro in averages from EventTime Windows -> out: <startTS,"boro avg(of boro in [startTS,startTS+winSize)" )
        SingleOutputStreamOperator<Tuple2<Long, String>> delaysNeighboroAverges = delaysNeighboro.keyBy(1).timeWindow(WINDOW_SIZE)
                .aggregate(new AverageAggregate(),new ProcessWindowFunction<Double, Tuple2<Long, String>, Tuple, TimeWindow>() {
                    //combine the average with window start times
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Double> elements, Collector<Tuple2<Long, String>> out) throws Exception {
                        String boroKey = tuple.getField(0);
                        Iterator<Double> all = elements.iterator();
                        Double avgOfBoro = all.next();//elements.iterator().next();    //expected only 1 element
                        Long startTimeOfWindow = context.window().getStart();
                        out.collect(new Tuple2<>(startTimeOfWindow, boroKey + CSV_SEP + avgOfBoro ));
                    }
                });
        if(FineTuneParallelism ) delaysNeighboroAverges.setParallelism(Integer.parseInt(prop.getProperty("q1_aggregate_parallelism")));
        //group output neighboro's averages by starting timeStamp and reduce merge tuple into output string
        SingleOutputStreamOperator<Tuple2<Long, String>> out = delaysNeighboroAverges
                .timeWindowAll(WINDOW_SIZE).reduce(new ReduceFunction<Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> reduce(Tuple2<Long, String> value1, Tuple2<Long, String> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + CSV_SEP + value2.f1 );
                    }
                });
        //present the output and put into file steram sink
        SingleOutputStreamOperator<String> outTsConverted= out.map(new ConvertTs());
        if(FineTuneParallelism) outTsConverted.setParallelism(Integer.parseInt(prop.getProperty("SINK_PARALLELISM")));
        DataStreamSink<String> sink = outTsConverted.addSink(Utils.fileOutputSink(prop.getProperty("OUT_PATH1")));
        if(FineTuneParallelism) sink.setParallelism(Integer.parseInt(prop.getProperty("SINK_PARALLELISM")));

        env.execute("Q1");
        end=System.nanoTime();
        System.out.println("elapsed: "+((double)(end-start))/1000000000);
    }



    private static class AscendingWatermarking extends AscendingTimestampExtractor<Tuple3<Long, String, Float>> {

        @Override
        public long extractAscendingTimestamp(Tuple3<Long, String, Float> element) {
            return element.f0;
        }
    }
}
