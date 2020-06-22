package nycBusDelays;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;


public class Query2 {

    private static final String CSV_LIST_SEP = " ";
    private static final String AM_START_TS_OFFSET = "09:00", AM_END_TS_OFFSET = "12:00";
    private static final String PM_START_TS_OFFSET = "12:00", PM_END_TS_OFFSET = "19:00";
    private static final String OUT_PATH = "csv2";
    public static int TOPN = 3;
    public static String HOSTNAME = "localhost";//"172.17.0.1";
    public static int PORT = 5555;
    public static Time WINDOW_SIZE = Time.days(7);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get input data by connecting to the socket
        DataStream<String> lines = env.socketTextStream(HOSTNAME, PORT, "\n", 1);
        //parse fields and assign timestamp
        DataStream<Tuple2<Long, String>> delayReasons = lines.map(new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String line) throws Exception {
                String[] fields = line.split("\\s");                            //"occurredOn","reason"
                try{
                    String x = fields[1];
                }catch (Exception e){
                    System.err.println(line+e);
                }
                return new Tuple2<>(Long.valueOf(fields[0]), fields[1]);
            }
        }).assignTimestampsAndWatermarks(new WatermarkingStrict());

        //separate delays information with respect to the specified time ranges (AM PM)
        DataStream<Tuple2<Long, String>> delayReasonsAM = delayReasons.filter(new FilterTimeRanges(AM_START_TS_OFFSET, AM_END_TS_OFFSET));
        DataStream<Tuple2<Long, String>> delayReasonsPM = delayReasons.filter(new FilterTimeRanges(PM_START_TS_OFFSET, PM_END_TS_OFFSET));

        /// for each time range rank the topN reasons of bus delays in time windows
        DataStream<Tuple2<Long, String>>[] delayTimeRanges = new DataStream[]{delayReasonsAM, delayReasonsPM};
        for (int i = 0; i < delayTimeRanges.length; i++) {
            DataStream<Tuple2<Long, String>> delayRange = delayTimeRanges[i];
            //count delays Reasons in time windows by appending 1 to each tuple + reduce summing counts
            //contains <winStartTS,reason,count>
            SingleOutputStreamOperator<Tuple3<Long, String, Long>> delayCounts = delayRange.map(new MapFunction<Tuple2<Long, String>, Tuple3<Long, String, Long>>() {
                @Override
                public Tuple3<Long, String, Long> map(Tuple2<Long, String> value) throws Exception {
                    return new Tuple3<>(value.f0, value.f1, 1L);
                }
            }).keyBy(1).timeWindow(WINDOW_SIZE).reduce(new ReduceFunction<Tuple3<Long, String, Long>>() {   //TODO SUM DIFF
                @Override
                public Tuple3<Long, String, Long> reduce(Tuple3<Long, String, Long> value1, Tuple3<Long, String, Long> value2) throws Exception {
                    return new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2);
                }
            }, new ProcessWindowFunction<Tuple3<Long, String, Long>, Tuple3<Long, String, Long>, Tuple, TimeWindow>() {
                @Override
                public void process(Tuple tuple, Context context, Iterable<Tuple3<Long, String, Long>> elements, Collector<Tuple3<Long, String, Long>> out) throws Exception {
                    tuple = elements.iterator().next();
                    out.collect(new Tuple3<>(context.window().getStart(), tuple.getField(1), tuple.getField(2)));
                }
            });
            //get the topN reasons by their count using a thread safe priority queue in tuple <winStartTs, "top1stReason ,top2ndReason...">
            //also round timestamps to the midnight of their associated day for later join different timeRange streams
            DataStream<Tuple2<Long, String>> reasonsRanked =delayCounts.keyBy(0).timeWindow(WINDOW_SIZE).aggregate(new RankReasons(TOPN, CSV_LIST_SEP));
            //TODO SCALABLE VERSION OF RANKING: pre ranking on sub key space -> final ranking on all partial results
            //divide the key space for each time win rank with respect of groups of reasons
            /*final int SUB_RANK_LEVEL=2;
            reasonsRanked = delayCounts.keyBy(new KeySelector<Tuple3<Long, String, Long>, Tuple2<Long, Integer>>() {
                @Override
                public Tuple2<Long, Integer> getKey(Tuple3<Long, String, Long> value) throws Exception {
                    return new Tuple2<>(value.f0, value.f1.hashCode() % SUB_RANK_LEVEL);
                }
            }).timeWindow(WINDOWSIZE).aggregate(PARTIAL RANK).keyBy(0).timeWindow(WINDOW_SIZE).aggregate(new RankReasons(TOPN, CSV_LIST_SEP));*/
            delayTimeRanges[i] = reasonsRanked; //save rank
        }

        //retrieve the ranked delays stream in the AM-PM time ranges
        //TODO MULTI TIME RANGE JOIN POSSIBILE WITH INTERMEDIATE JOIN STERAM VAR+FINAL MAP (that final map ts-> str included in the join func)
        delayReasonsAM=delayTimeRanges[0];
        delayReasonsPM=delayTimeRanges[1];

        //join the ranked delays for the final output
        DataStream<Tuple2<String, String>> joinedStream=delayReasonsAM.join(delayReasonsPM).where(new TSKeySelector()).equalTo(new TSKeySelector())
        .window(TumblingEventTimeWindows.of(WINDOW_SIZE)).apply(new JoinFunction<Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<String,String>>() {
                @Override
                public Tuple2<String, String> join(Tuple2<Long, String> am, Tuple2<Long, String> pm) throws Exception {
                    String startTimeStamp= Utils.convertTs(am.f0,true);
                    return new Tuple2<>(startTimeStamp,"AM"+CSV_LIST_SEP+am.f1+CSV_LIST_SEP+"PM"+CSV_LIST_SEP+pm.f1);
                }
            });


        joinedStream.addSink(Utils.fileOutputSink(OUT_PATH)).setParallelism(1);
        env.execute("Q2");
    }

    /**
     * Filter a stream of time-stamped tuple
     * keep the tuples witch has time that fall into a specified range expressed as offset from the midnight
     */
    static class FilterTimeRanges implements FilterFunction<Tuple2<Long, String>> {
        private LocalTime timeRangeStart, timeRangeEnd;    //time range to filter tuple that fall in [start,end)

        public FilterTimeRanges(String start, String end){
            timeRangeStart=LocalTime.parse(start);
            timeRangeEnd=LocalTime.parse(end);
        }

        @Override
        public boolean filter(Tuple2<Long, String> tuple) throws Exception { //True if the timestamp field of the tuple fall in the given timerange
            LocalTime tupleTime=LocalDateTime.ofInstant(Instant.ofEpochMilli(tuple.f0), ZoneOffset.UTC).toLocalTime();
            return timeRangeStart.isBefore(tupleTime) && tupleTime.isBefore(timeRangeEnd);
        }
    }

    static class WatermarkingStrict implements AssignerWithPunctuatedWatermarks<Tuple2<Long, String>> {

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple2<Long, String> lastElement, long extractedTimestamp) {
            return new Watermark(lastElement.f0);
        }

        @Override
        public long extractTimestamp(Tuple2<Long, String> element, long previousElementTimestamp) {
            return element.f0;
        }
    }

    static class TSKeySelector implements KeySelector<Tuple2<Long,String>,Long>{    //extract the time stamp field as stream key

        @Override
        public Long getKey(Tuple2<Long, String> value) throws Exception {
            return value.f0;
        }
    }
}
