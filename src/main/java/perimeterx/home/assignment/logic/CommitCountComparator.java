package perimeterx.home.assignment.logic;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compare 2 tuple entries
 * We don't care about the "_1()" of the tuple
 * We compare only the "_2()" which is a long value (representing commit count)
 */
public class CommitCountComparator<T> implements Serializable, Comparator<Tuple2<T, Long>> {
    @Override
    public int compare(Tuple2<T, Long> lhs, Tuple2<T, Long> rhs) {
        return lhs._2().compareTo(rhs._2());
    }
}
