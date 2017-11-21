package perimeterx.home.assignment.bean;


import java.io.Serializable;
import java.time.DayOfWeek;

public class PctDow implements Serializable {
    public static final PctDow EMPTY = new PctDow(null, 0);
    private final DayOfWeek dow;
    private final double pct;

    public PctDow(DayOfWeek dow, double pct) {
        this.dow = dow;
        this.pct = pct;
    }

    public DayOfWeek getDow() {
        return dow;
    }

    public double getPct() {
        return pct;
    }
}
