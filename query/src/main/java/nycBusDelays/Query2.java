package nycBusDelays;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Iterator;


public class Query2 {

    private static final String CSV_LIST_SEP = " ";
    private static final String AM_START = "05:00", AM_END = "11:59";
    private static final String PM_START = "12:00", PM_END = "19:00";
    private static final String OUT_PATH = "csv2";
    private static final String CSV_SEP = ",";
    public static int TOPN = 3;
    public static String HOSTNAME = "localhost";//"172.17.0.1";
    public static int PORT = 5555;
    public static Time WINDOW_SIZE = Time.days(7);

    public static void main(String[] args) throws Exception {

        long end,start=System.nanoTime();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get input data by connecting to the socket
        DataStream<String> lines = env.socketTextStream(HOSTNAME, PORT, "\n", 1);
        //parse fields and assign timestamp
        DataStream<Tuple2<Long, String>> delayReasons = lines.map(new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String line) throws Exception {
                String[] fields = line.split("\\s");                            //"occurredOn","reason"
                return new Tuple2<>(Long.valueOf(fields[0]), fields[1]);
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, String>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<Long, String> element) {
                return element.f0;
            }
        });

        //separate delays information with respect to the specified time ranges (AM PM)
        DataStream<Tuple2<Long, String>> delayReasonsAM = delayReasons.filter(new FilterTimeRanges(AM_START, AM_END));
        DataStream<Tuple2<Long, String>> delayReasonsPM = delayReasons.filter(new FilterTimeRanges(PM_START, PM_END));

        /// for each time range rank the topN reasons of bus delays in time windows
        DataStream<Tuple2<Long, String>>[] delayTimeRanges = new DataStream[]{delayReasonsAM, delayReasonsPM};
        for (int i=0;i<delayTimeRanges.length;i++){
            DataStream<Tuple2<Long, String>> delayRange=delayTimeRanges[i];

            //count delays Reasons in time windows by appending 1 to each tuple + reduce summing counts
            //<winStartTS,reason,count>
            SingleOutputStreamOperator<Tuple3<Long, String, Long>> delayCounts = delayRange.map(new MapFunction<Tuple2<Long, String>, Tuple3<Long, String, Long>>() {
                @Override
                public Tuple3<Long, String, Long> map(Tuple2<Long, String> value) throws Exception {
                    return new Tuple3<>(value.f0, value.f1, 1L);
                }
            }).keyBy(1).timeWindow(WINDOW_SIZE).reduce(new ReduceFunction<Tuple3<Long, String, Long>>() { //sum 1s
                @Override
                public Tuple3<Long, String, Long> reduce(Tuple3<Long, String, Long> value1, Tuple3<Long, String, Long> value2) throws Exception {
                    return new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2);
                }
            }, new ProcessWindowFunction<Tuple3<Long, String, Long>, Tuple3<Long, String, Long>, Tuple, TimeWindow>() {
                @Override
                public void process(Tuple key, Context context, Iterable<Tuple3<Long, String, Long>> elements, Collector<Tuple3<Long, String, Long>> out) throws Exception {
                    Iterator<Tuple3<Long, String, Long>> all = elements.iterator();
                    Tuple3<Long,String,Long> tuple = all.next();
                    if (all.hasNext()) System.err.println("FK REDUCE");
                    out.collect(new Tuple3<>(context.window().getStart(), tuple.f1  , tuple.f2));
                }
            });

            //get the topN reasons using a RedBlack tree struct obtaining tuples like <winStartTs, "top1stReason ,top2ndReason...">
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
        delayReasonsAM=delayTimeRanges[0];
        delayReasonsPM=delayTimeRanges[1];
//        delayReasonsAM.map(new MapFunction<Tuple2<Long, String>, String>() {
//            @Override
//            public String map(Tuple2<Long, String> value) throws Exception {
//                return Utils.convertTs(value.f0,false)+value.f1;
//            }
//        }).print().setParallelism(1);
        //join the ranked delays for the final output
        DataStream<String> joinedStream=delayReasonsAM.join(delayReasonsPM).where(new TSKeySelector()).equalTo(new TSKeySelector())
        .window(TumblingEventTimeWindows.of(WINDOW_SIZE)).apply(new JoinFunction<Tuple2<Long, String>, Tuple2<Long, String>,String>() {
                @Override
                public String join(Tuple2<Long, String> am, Tuple2<Long, String> pm) throws Exception {
                    String startTimeStamp= Utils.convertTs(am.f0,true);
                    return startTimeStamp+CSV_SEP+"AM"+CSV_LIST_SEP+am.f1+CSV_LIST_SEP+"PM"+CSV_LIST_SEP+pm.f1;
                }
            });


        joinedStream.addSink(Utils.fileOutputSink(OUT_PATH)).setParallelism(1);
        env.execute("Q2");
        end=System.nanoTime();
        System.out.println("elapsed: "+((double)(end-start))/1000000000);
    }

    /**
     * Filter a stream of time-stamped tuple looking if the time stamp is in a given time range
     */
    static class FilterTimeRanges implements FilterFunction<Tuple2<Long, String>> {
        private LocalTime timeRangeStart, timeRangeEnd;    //time range to filter tuple that fall in [start,end)

        public FilterTimeRanges(String start, String end){
            timeRangeStart=LocalTime.parse(start);
            timeRangeEnd=LocalTime.parse(end);
        }

        @Override
        public boolean filter(Tuple2<Long, String> tuple) {
            //True if the timestamp field of the tuple fall in the given time range,extreems included
            LocalTime tupleTime=LocalDateTime.ofInstant(Instant.ofEpochMilli(tuple.f0), ZoneOffset.UTC).toLocalTime();
            return timeRangeStart.compareTo(tupleTime)<=0 && tupleTime.compareTo(timeRangeEnd)<=0;
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
