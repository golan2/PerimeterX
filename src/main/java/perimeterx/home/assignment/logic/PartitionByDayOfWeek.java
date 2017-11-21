package perimeterx.home.assignment.logic;

import org.apache.spark.Partitioner;

import java.time.DayOfWeek;

public class PartitionByDayOfWeek extends Partitioner {

    @Override
    public int numPartitions() {
        return 7;
    }

    @Override
    public int getPartition(Object key) {
        return ((DayOfWeek) key).getValue() - 1;
    }

}
