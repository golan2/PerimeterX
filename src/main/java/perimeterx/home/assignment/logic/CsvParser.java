package perimeterx.home.assignment.logic;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * Example:
 * date,name
 * 2016-08-28 10:26:09 UTC,Timm Friebe
 * 2016-03-29 23:14:33 UTC,Grace Lee
 * 2016-06-21 09:38:55 UTC,SuperDJ
 * 2016-04-14 06:46:52 UTC,liuxp
 *
 */
public class CsvParser implements PairFunction<String, LocalDate, String> {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'");

    @Override
    public Tuple2<LocalDate, String> call(String s) throws Exception {
        if (s.startsWith("date,name")) return new Tuple2<>(LocalDate.MAX, "HEADERLINE");
        final String[] split = s.split(",");
        return new Tuple2<>(LocalDate.parse(split[0].trim(), formatter), split[1].trim());
    }
}
