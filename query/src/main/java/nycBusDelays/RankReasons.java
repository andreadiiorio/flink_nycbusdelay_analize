package nycBusDelays;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.*;

/**
 * Flink Aggregation function to rank a stream of tuple containing a key and a count value
 * rolling ranking using the red black tree based struct TreeSet costrained at fixed size (size of rank=k)
 * so each insert cost O(log k) -> TOP K ranking in O(n log(k))
 */
public class RankReasons implements AggregateFunction<Tuple3<Long, String, Long>, TreeSet<Tuple3<Long, String, Long>>, Tuple2<Long, String>> {
        private int TOPN;
        private String CSV_SEP;

        public RankReasons(int TOPN,  String CSV_SEP) {
                this.TOPN=TOPN;
                this.CSV_SEP = CSV_SEP;
        }

        @Override
        public TreeSet<Tuple3<Long, String, Long>> createAccumulator() {
                return new TreeSet<>(new Comparator<Tuple3<Long, String, Long>>() {
                        @Override
                        public int compare(Tuple3<Long, String, Long> o1, Tuple3<Long, String, Long> o2) {
                                int cmpCounts=Long.compare(o1.f2,o2.f2);
                                if (cmpCounts==0)       return 1;       //keep duplicate values
                                return cmpCounts;
                        }
                });

        }

        @Override
        public TreeSet<Tuple3<Long, String, Long>> add(Tuple3<Long, String, Long> value, TreeSet<Tuple3<Long, String, Long>> accumulator) {
                accumulator.add(value);
                if (accumulator.size()>TOPN)    accumulator.pollFirst(); //keep just the topN elements -> O(n log k ) for the topK ranking
                return accumulator;
        }

        @Override
        public Tuple2<Long, String> getResult(TreeSet<Tuple3<Long, String, Long>> accumulator) {
                //concat top reasons extracting the priorityQueue head multiple times
                String outRanks = "";
                Tuple3<Long, String, Long> head = null;
                int topNRanked = accumulator.size();
                for (int x = 0; x < topNRanked; x++) {
                        head = accumulator.pollLast();
                        if (x+1==topNRanked)  outRanks += head.f1 + CSV_SEP + head.f2;
                        else                  outRanks += head.f1 + CSV_SEP + head.f2+CSV_SEP ;
                }
                return new Tuple2<>(head.f0, outRanks);

        }

        @Override
        public TreeSet<Tuple3<Long, String, Long>> merge(TreeSet<Tuple3<Long, String, Long>> a, TreeSet<Tuple3<Long, String, Long>> b) {
                a.addAll(b);
                //resize the merged treeSet to keep the topK
                int extraElements=a.size()-TOPN;
                for (int x = 0; x < extraElements; x++)       a.pollLast();
                return a;

        }


}
