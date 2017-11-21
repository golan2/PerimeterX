package perimeterx.home.assignment.logic;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.time.DayOfWeek;
import java.time.LocalDate;

public class MapDateToDayOfWeek implements PairFunction<Tuple2<LocalDate, Long>, DayOfWeek, Long> {
    @Override
    public Tuple2<DayOfWeek, Long> call(Tuple2<LocalDate, Long> t) throws Exception {
        return new Tuple2<>(t._1().getDayOfWeek(), t._2());
    }
}
