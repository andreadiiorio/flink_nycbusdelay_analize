package nycBusDelays;

import org.apache.flink.api.common.functions.*;
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

import static nycBusDelays.Utils.convertTs;


public class Query2 {

    private static final String CSV_SEP = ",";
    private static final String AM_START = "05:00", AM_END = "11:59";
    private static final String PM_START = "12:00", PM_END = "19:00";
    private static final String OUT_PATH = "csv2";
    private static final short AM_STREAM_ID = 0,PM_STREAM_ID = 1;
    public static int TOPN = 3;
    public static String HOSTNAME = "localhost";//"172.17.0.1";
    public static int PORT = 5555;
    public static Time WINDOW_SIZE = Time.hours(24);

    public static void main(String[] args) throws Exception {

        long end, start = System.nanoTime();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(1);
        // get input data by connecting to the socket
        DataStream<String> lines = env.socketTextStream(HOSTNAME, PORT, "\n", 1);
        //parse fields and assign timestamp
        DataStream<Tuple2<Long, String>> delayReasons = lines.map(new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String line) throws Exception {
                String[] fields = line.split("\\s");                            //"occurredOn","reason"
                return new Tuple2<>(Long.valueOf(fields[0]), fields[1]);
            }
        }).assignTimestampsAndWatermarks(new WatermarkingStrict());
        //separate delays information with respect to the specified time ranges (AM PM)
        DataStream<Tuple2<Long, String>> delayReasonsAM = delayReasons.filter(new FilterTimeRanges(AM_START, AM_END));
        DataStream<Tuple2<Long, String>> delayReasonsPM = delayReasons.filter(new FilterTimeRanges(PM_START, PM_END));
        DataStream<Tuple3<Long,Short,String>> delayReasonAMRanked=null,delayReasonPMRanked=null;
        /// for each time range rank the topN reasons of bus delays in time windows
        DataStream<Tuple2<Long, String>>[] delayTimeRanges = new DataStream[]{delayReasonsAM, delayReasonsPM};
        for (int i = 0; i < delayTimeRanges.length; i++) {
            DataStream<Tuple2<Long, String>> delayRange = delayTimeRanges[i];

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
                    Tuple3<Long, String, Long> tuple = elements.iterator().next();
                    out.collect(new Tuple3<>(context.window().getStart(), tuple.f1, tuple.f2));
                }
            });

            //get the topN reasons using a RedBlack tree struct obtaining tuples like <winStartTs, "top1stReason ,top2ndReason...">
            //also round timestamps to the midnight of their associated day for later join different timeRange streams
            DataStream<Tuple3<Long, Short, String>> reasonsRanked = delayCounts.keyBy(0).timeWindow(WINDOW_SIZE).aggregate(new RankReasons(TOPN, (short) i, CSV_SEP));
            //assign the result to the corrisponding time range

            if(i==AM_STREAM_ID) delayReasonAMRanked=reasonsRanked;
            else                delayReasonPMRanked=reasonsRanked;
        }

        //outer join the ranked delays for the final output
        DataStream<String> joinedStream = delayReasonAMRanked.union(delayReasonPMRanked).keyBy(0).timeWindow(WINDOW_SIZE)
                .aggregate(new AggregateFunction<Tuple3<Long, Short, String>, Tuple3<Long,String,String>, String>() {
                    @Override
                    public Tuple3<Long, String, String> createAccumulator() {
                        return new Tuple3<>(0L,"","");
                    }

                    @Override
                    public Tuple3<Long, String, String> add(Tuple3<Long, Short, String> value, Tuple3<Long, String, String> accumulator) {
                        //set accumulator am/pm field with the new tuple
                        if(value.f0> accumulator.f0)    accumulator.f0=value.f0;    //set time stamp for first accumulator run
                        if (value.f1==AM_STREAM_ID) {
                            accumulator.f1=value.f2;
                            return accumulator;
                        }
                        else{
                            accumulator.f2=value.f2;
                            return accumulator;
                        }
                    }

                    @Override
                    public String getResult(Tuple3<Long, String, String> accumulator) {
                        return Utils.convertTs(accumulator.f0,true)+
                                CSV_SEP+"AM"+ CSV_SEP+accumulator.f1+ CSV_SEP+"PM"+ CSV_SEP+accumulator.f2;
                    }

                    @Override
                    public Tuple3<Long, String, String> merge(Tuple3<Long, String, String> a, Tuple3<Long, String, String> b) {
                        //merge into a missing field
                        if(b.f0>a.f0)                   a.f0=b.f0;
                        if(b.f1.length()>a.f1.length()) a.f1=b.f1;
                        if(b.f2.length()>a.f2.length()) a.f2=b.f2;
                        return a;
                    }
                });


        joinedStream.addSink(Utils.fileOutputSink(OUT_PATH)).setParallelism(1);
        env.execute("Q2");
        end = System.nanoTime();
        System.out.println("elapsed: " + ((double) (end - start)) / 1000000000);
    }

    /**
     * Filter a stream of time-stamped tuple looking if the time stamp is in a given time range
     */
    static class FilterTimeRanges implements FilterFunction<Tuple2<Long, String>> {
        private LocalTime timeRangeStart, timeRangeEnd;    //time range to filter tuple that fall in [start,end)

        public FilterTimeRanges(String start, String end) {
            timeRangeStart = LocalTime.parse(start);
            timeRangeEnd = LocalTime.parse(end);
        }

        @Override
        public boolean filter(Tuple2<Long, String> tuple) {
            //True if the timestamp field of the tuple fall in the given time range,extreems included
            LocalTime tupleTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(tuple.f0), ZoneOffset.UTC).toLocalTime();
            return timeRangeStart.compareTo(tupleTime) <= 0 && tupleTime.compareTo(timeRangeEnd) <= 0;
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

    static class TSKeySelector implements KeySelector<Tuple2<Long, String>, Long> {    //extract the time stamp field as stream key

        @Override
        public Long getKey(Tuple2<Long, String> value) throws Exception {
            return value.f0;
        }
    }
}
