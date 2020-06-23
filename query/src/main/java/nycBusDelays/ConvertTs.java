package nycBusDelays;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import javax.xml.stream.FactoryConfigurationError;

public class ConvertTs implements MapFunction<Tuple2<Long, String>, String> {
    @Override
    public String map(Tuple2<Long, String> value) {
        return Utils.convertTs(value.f0, false)+","+value.f1;
    }
}
