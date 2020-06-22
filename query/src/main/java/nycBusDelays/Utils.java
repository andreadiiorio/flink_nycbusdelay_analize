package nycBusDelays;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.TimeZone;

public class Utils {
    ///Properties
    public static String PROPERTIES_DEFAULT_PATH="";
    public static Properties properties;
    public static void loadPropertiesFile(String propertiesPath) throws IOException {
        properties.load(new FileInputStream(propertiesPath));
    }

    /// time conversion-rounding support
    public static final ZoneId ZONE_ID_NYC = ZoneId.of("America/New_York");
    private static final DateTimeFormatter dateStrFormatterDayGranularity=DateTimeFormatter.ofPattern("dd-MM-yyyy");
    private static final DateTimeFormatter dateStrFormatter=DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    public static String convertTs(Long timeStamp,boolean dayGranularity) {    //TimeStamp -> string
        //if given dayGranularity returned string indicating the day of the timestamp, otherwise use a seconds granularity
        LocalDateTime date=LocalDateTime.ofInstant(Instant.ofEpochMilli(timeStamp),ZONE_ID_NYC);
        if (dayGranularity) return date.format(dateStrFormatterDayGranularity);
        else                return date.format(dateStrFormatter);
    }

    /**
     * round down the given timestamp to the timestamp of the midnight of the day that contain the timestamp
     * @param timestamp to round to its day's midnight
     * @return the timestamp rounded down to midnight in milliseconds from epoch
     */
    public static Long roundTsDownMidnight(Long timestamp) {
        LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),ZONE_ID_NYC);
        LocalDateTime midnight=date.truncatedTo(ChronoUnit.DAYS);
        return midnight.atZone(ZONE_ID_NYC).toEpochSecond()*1000;
    }
    public static void main(String[] args) throws Exception {
        long ts=1452230280000L;
    }
}
