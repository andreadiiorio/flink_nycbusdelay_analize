package nycBusDelays;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * minimal aggregate function to compute the average value of the third field of a stream of tuple
 */
public class AverageAggregate
        implements AggregateFunction<Tuple3<Long, String, Float>, Tuple2<Double, Long>, Double> {
    @Override
    public Tuple2<Double, Long> createAccumulator() {
        return new Tuple2<>(0d,0L);
    }

    @Override
    public Tuple2<Double, Long> add(Tuple3<Long, String, Float> value, Tuple2<Double, Long> accumulator) {
        return new Tuple2<>(accumulator.f0+value.f2,accumulator.f1+1L);
    }

    @Override
    public Double getResult(Tuple2<Double, Long> accumulator) {
        return ((double) accumulator.f0) / accumulator.f1;
    }

    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}
