package nycBusDelays;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 *
 */
public class Query1 {
    private static final String CSV_LIST_SEP = " ";
    public static String HOSTNAME = "localhost";//"172.17.0.1";
    public static int PORT = 5555;
    public static Time WINDOW_SIZE = Time.hours(24);
    private static int SINK_PARALLELISM = 1;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get input data by connecting to the socket
        DataStream<String> lines = env.socketTextStream(HOSTNAME, PORT, "\n");
        //parse and extract timestamp
        DataStream<Tuple3<Long, String, Float>> delaysNeighboro = lines.map(new MapFunction<String, Tuple3<Long,String,Float>>() {
            @Override
            public Tuple3<Long, String, Float> map(String line) throws Exception {

                String[] fields = line.split("\\s");                            //"occurredOn","boro","howLongDelayed"return null;
                return new Tuple3(Long.valueOf(fields[0]), fields[1], Float.valueOf(fields[2]));
            } //extract timestamp from first field
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, String, Float>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<Long, String, Float> element) {
                        return element.f0;
                    }
                });
        WindowedStream<Tuple3<Long, String, Float>, Tuple, TimeWindow> delaysNeighboroWin = delaysNeighboro.keyBy(1).timeWindow(WINDOW_SIZE);
//        delaysNeighboroWin.minBy(2).print().setParallelism(1);        env.execute("Q1");        System.exit(1);
        ///aggregate delays per boro in averages from EventTime Windows -> out: <startTS,"boro avg(of boro in [startTS,startTS+winSize)" )
        DataStream<Tuple2<Long, String>> delaysNeighboroAverges = delaysNeighboroWin.aggregate(new AverageAggregate(),
                new ProcessWindowFunction<Double, Tuple2<Long, String>, Tuple, TimeWindow>() {
                    //combine the average with window start times
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Double> elements, Collector<Tuple2<Long, String>> out) throws Exception {
                        String boroKey = tuple.getField(0);
                        Iterator<Double> all = elements.iterator();
                        Double avgOfBoro = all.next();//elements.iterator().next();    //expected only 1 element
                        if (all.hasNext())  System.err.println("NOT 1! AGGREGATED ELEMENT"+all.next());    //TODO DEBUG CHECK
                        Long startTimeOfWindow = context.window().getStart();
                        out.collect(new Tuple2<>(startTimeOfWindow, boroKey + CSV_LIST_SEP + avgOfBoro+ CSV_LIST_SEP));
                    }
                });
        DataStream<Tuple2<Long, String>> out = delaysNeighboroAverges.keyBy(0).timeWindow(WINDOW_SIZE).reduce(new ReduceFunction<Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> reduce(Tuple2<Long, String> value1, Tuple2<Long, String> value2) throws Exception {
                return new Tuple2<>(value1.f0,value1.f1+CSV_LIST_SEP+value2.f1+CSV_LIST_SEP);
            }
        });
        /*.reduce(new ReduceFunction<Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> reduce(Tuple2<Long, String> value1, Tuple2<Long, String> value2) throws Exception {
                if (! value1.f0.equals(value2.f0)) System.err.println("FKK KEYS" + value1.f0 + " " + value2.f0); //TODO DEBUG CHECK
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });
//        SingleOutputStreamOperator<Tuple2<Long, String>> outt = delaysNeighboroAverges.keyBy(0).sum(0);
*/
        delaysNeighboroAverges.print().setParallelism(SINK_PARALLELISM);
        env.execute("Q1");
    }

    ;
}
/*

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		*/
/*
 * Here, you can start creating your execution plan for Flink.
 *
 * Start with getting some data from the environment, like
 * 	env.readTextFile(textPath);
 *
 * then, transform the resulting DataStream<String> using operations
 * like
 * 	.filter()
 * 	.flatMap()
 * 	.join()
 * 	.coGroup()
 *
 * and many more.
 * Have a look at the programming guide for the Java API:
 *
 * https://flink.apache.org/docs/latest/apis/streaming/index.html
 *
 */