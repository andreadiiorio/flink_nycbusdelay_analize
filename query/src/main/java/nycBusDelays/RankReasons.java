package nycBusDelays;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.*;


public class RankReasons implements AggregateFunction<Tuple3<Long, String, Long>, TreeSet<Tuple3<Long, String, Long>>, Tuple2<Long, String>> {
        private int TOPN;
        private String CSV_LIST_SEP;

        public RankReasons(int TOPN,  String CSV_LIST_SEP) {
                this.TOPN=TOPN;
                this.CSV_LIST_SEP=CSV_LIST_SEP;
        }

        @Override
        public TreeSet<Tuple3<Long, String, Long>> createAccumulator() {
                return new TreeSet<>(new Comparator<Tuple3<Long, String, Long>>() {
                        @Override
                        public int compare(Tuple3<Long, String, Long> o1, Tuple3<Long, String, Long> o2) {
                                return Long.compare(o1.f2,o2.f2);
                        }
                });

        }

        @Override
        public TreeSet<Tuple3<Long, String, Long>> add(Tuple3<Long, String, Long> value, TreeSet<Tuple3<Long, String, Long>> accumulator) {
                accumulator.add(value);
                if (accumulator.size()>TOPN)    accumulator.pollLast(); //keep just the topN elements -> O(n log k ) for the topK ranking
                return accumulator;
        }

        @Override
        public Tuple2<Long, String> getResult(TreeSet<Tuple3<Long, String, Long>> accumulator) {
                //concat top reasons extracting the priorityQueue head multiple times
                String outRanks = "";
                Tuple3<Long, String, Long> head = null;
                //System.out.println("Size:"+accumulator.size());
                for (int x = 0; x < accumulator.size(); x++) {
                        head = accumulator.pollFirst();
                        outRanks += head.f1 + CSV_LIST_SEP + head.f2 + CSV_LIST_SEP;    //TODO ALSO COUNT ADDED
                }
                //build the rank with the concatenated reasons ranked + starting common timestamp  rounded down to the midnight of the associated day
                Long dayRoundedDownTs= Utils.roundTsDownMidnight(head.f0);
                return new Tuple2<>(dayRoundedDownTs, outRanks);

        }

        @Override
        public TreeSet<Tuple3<Long, String, Long>> merge(TreeSet<Tuple3<Long, String, Long>> a, TreeSet<Tuple3<Long, String, Long>> b) {
                a.addAll(b);
                int extraElements=a.size()-TOPN;
                for (int x=0;x<extraElements && extraElements>0;x++)       a.pollLast();   //resize the merged treeSet to keep the topK
                return a;

        }


}