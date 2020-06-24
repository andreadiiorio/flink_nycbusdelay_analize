package nycBusDelays;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Utils {
    ///Properties
    private static Properties properties=null;
    private static Utils utils;
    private Utils(){}
    public static Utils getUtils(){
        if (utils==null) utils=new Utils();
        return utils;
    }
    public Properties getProperty() throws IOException {
        if (properties==null) {
            properties = new Properties();
            properties.load(getClass().getClassLoader().getResourceAsStream("config.properties"));
        }
        return properties;
    }

    /// time conversion-rounding support
    private static final DateTimeFormatter dateStrFormatterDayGranularity=DateTimeFormatter.ofPattern("dd-MM-yyyy");
    private static final DateTimeFormatter dateStrFormatter=DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    public static String convertTs(Long timeStamp,boolean dayGranularity) {    //TimeStamp -> string
        //if given dayGranularity returned string indicating the day of the timestamp, otherwise use a seconds granularity
        LocalDateTime date=LocalDateTime.ofInstant(Instant.ofEpochMilli(timeStamp),ZoneOffset.UTC); //nyc utc offset already inserted at the source
        if (dayGranularity) return date.format(dateStrFormatterDayGranularity);
        else                return date.format(dateStrFormatter);
    }

    /**
     * round down the given timestamp to the timestamp of the midnight of the day that contain the timestamp
     * @param timestamp to round to its day's midnight
     * @return the timestamp rounded down to midnight in milliseconds from epoch
     */
    public static Long roundTsDownMidnight(Long timestamp) {
        LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),ZoneOffset.UTC);
        LocalDateTime midnight=date.truncatedTo(ChronoUnit.DAYS);
        return midnight.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    //flink quick sinks
    public static <T> StreamingFileSink<T> fileOutputSink(String outPath){

        return  StreamingFileSink.forRowFormat(new Path(outPath),new SimpleStringEncoder<T>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
    }
    public static void main(String[] args) throws Exception {
        long ts=1441092540000L;
        System.out.println(convertTs(ts,true));
        System.out.println(convertTs(ts,false));
        System.out.println(convertTs(roundTsDownMidnight(ts),true));


    }
}
