package nycBusDelays;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
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
import java.util.Properties;

import static nycBusDelays.Utils.convertTs;


public class Query2 {


    public static void main(String[] args) throws Exception {
        //init vars
        final Properties prop = Utils.getUtils().getProperty();
        final String CSV_SEP = prop.getProperty("CSV_SEP");
        Time WINDOW_SIZE;
        if (prop.containsKey("WIN_HOURS")) WINDOW_SIZE = Time.hours(Integer.parseInt(prop.getProperty("WIN_HOURS")));
        else WINDOW_SIZE = Time.days(Integer.parseInt(prop.getProperty("WIN_DAYS")));
        boolean FineTuneParallelism = false;
        if (Boolean.parseBoolean(prop.getProperty("FineTuneParallelism"))) FineTuneParallelism = true;
        long end, start = System.nanoTime();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get input data by connecting to the socket
        DataStream<String> lines = env.socketTextStream(prop.getProperty("HOSTNAME"), Integer.parseInt(prop.getProperty("PORT")), "\n", 1);
        //parse fields and assign timestamp
        SingleOutputStreamOperator<Tuple2<Long, String>> delayReasons = lines.map(new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String line) throws Exception {
                String[] fields = line.split("\\s");                            //"occurredOn","reason"
                return new Tuple2<>(Long.valueOf(fields[0]), fields[1]);
            }
        });
        if (FineTuneParallelism) delayReasons.setParallelism(Integer.parseInt(prop.getProperty("q2_map_parallelism")));
        delayReasons = delayReasons.assignTimestampsAndWatermarks( new AscendingWatermarking());
        if (FineTuneParallelism) delayReasons.setParallelism(Integer.parseInt(prop.getProperty("q2_map_parallelism")));

        //separate delays information with respect to the specified time ranges (AM PM)
        DataStream<Tuple2<Long, String>> delayReasonsAM =
                delayReasons.filter(new FilterTimeRanges(prop.getProperty("AM_START"), prop.getProperty("AM_END")));
        DataStream<Tuple2<Long, String>> delayReasonsPM =
                delayReasons.filter(new FilterTimeRanges(prop.getProperty("PM_START"), prop.getProperty("PM_END")));

        /// for each time range stream, rank the topN reasons of bus delays in time windows
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
            });
            if (FineTuneParallelism) delayCounts.setParallelism(Integer.parseInt(prop.getProperty("q2_count")));

            //get the topN reasons using a RedBlack tree based struct obtaining tuples like <winStartTs, "top1stReason ,top2ndReason...">
            //also round timestamps to the midnight of their associated day for later outer join
            SingleOutputStreamOperator<Tuple2<Long, String>> reasonsRanked = delayCounts.timeWindowAll(WINDOW_SIZE)
                    .aggregate(new RankReasons(Integer.parseInt(prop.getProperty("TOPN")), CSV_SEP),
                            new ProcessAllWindowFunction<Tuple2<Long, String>, Tuple2<Long, String>, TimeWindow>() {    //initial timestamp <= winStart down to 00:00
                                @Override
                                public void process(Context context, Iterable<Tuple2<Long, String>> elements, Collector<Tuple2<Long, String>> out) throws Exception {
                                    out.collect(new Tuple2<>(Utils.roundTsDownMidnight(context.window().getStart()), elements.iterator().next().f1));
                                }
                            });
            delayTimeRanges[i] = reasonsRanked; //save rank
        }

        //retrieve the ranked delays stream in the AM-PM time ranges
        delayReasonsAM = delayTimeRanges[0];
        delayReasonsPM = delayTimeRanges[1];
        //outer join the ranked delays for the final output
        DataStream<String> joinedStream = delayReasonsAM.coGroup(delayReasonsPM).where(new TSKeySelector()).equalTo(new TSKeySelector())
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE)).apply(new CoGroupFunction<Tuple2<Long, String>, Tuple2<Long, String>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Long, String>> am, Iterable<Tuple2<Long, String>> pm, Collector<String> out) throws Exception {

                        boolean amAvaible = am.iterator().hasNext(), pmAvaible = pm.iterator().hasNext();
                        String outTuple = "";
                        if (amAvaible) outTuple = convertTs(am.iterator().next().f0, true) + CSV_SEP +
                                "AM" + CSV_SEP + am.iterator().next().f1 + CSV_SEP + "PM" + CSV_SEP;
                        else {
                            out.collect(convertTs(pm.iterator().next().f0, true) + CSV_SEP
                                    + "AM" + CSV_SEP + "PM" + CSV_SEP + pm.iterator().next().f1);
                            return;
                        }
                        if (pmAvaible) outTuple += pm.iterator().next().f1;
                        out.collect(outTuple);
                    }
                });

        joinedStream.addSink(Utils.fileOutputSink(prop.getProperty("OUT_PATH2"))).setParallelism(Integer.parseInt(prop.getProperty("SINK_PARALLELISM")));
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
    //ascending watermarking
    static class AscendingWatermarking extends AscendingTimestampExtractor<Tuple2<Long, String>> {
        @Override
        public long extractAscendingTimestamp(Tuple2<Long, String> element) {
            return element.f0;
        }
    }
    //extract the time stamp field as stream key
    static class TSKeySelector implements KeySelector<Tuple2<Long, String>, Long> {

        @Override
        public Long getKey(Tuple2<Long, String> value) throws Exception {
            return value.f0;
        }
    }
}
