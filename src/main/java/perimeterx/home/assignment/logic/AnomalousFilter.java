package perimeterx.home.assignment.logic;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.Map;

public class AnomalousFilter implements Function<Tuple2<LocalDate, Long>, Boolean> {
    private final Map<DayOfWeek, Double> avgMap;
    private final Map<DayOfWeek, Double> stdDevMap;

    public AnomalousFilter(Map<DayOfWeek, Double> avgMap, Map<DayOfWeek, Double> stdDevMap) {
        this.avgMap = avgMap;
        this.stdDevMap = stdDevMap;
    }

    @Override
    public Boolean call(Tuple2<LocalDate, Long> tuple) throws Exception {
        final DayOfWeek dow = tuple._1().getDayOfWeek();
        final Double avg = avgMap.get(dow);
        final Double std = stdDevMap.get(dow);
        final Long commitCount = tuple._2();
        return (commitCount > avg + std*2);
    }

}
