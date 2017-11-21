package perimeterx.home.assignment;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import perimeterx.home.assignment.logic.AnomalousFilter;
import perimeterx.home.assignment.logic.CommitCountComparator;
import perimeterx.home.assignment.logic.CsvParser;
import perimeterx.home.assignment.logic.MapDateToDayOfWeek;
//import perimeterx.home.assignment.logic.PartitionByDayOfWeek;
import scala.Tuple2;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;

public class GitHubCommits {
    private static final String BAR = String.join("", Collections.nCopies(25, "="));
    private static final String FILENAME = "de_home_task_input.csv";

    public static void main(String[] args) {

        //Read the file "as-is"
        final JavaSparkContext               sparkContext = createSparkContext();
        final String                         fileName     = getFileName(args);
        final JavaRDD<String>                rdd          = sparkContext.textFile(fileName);
        final JavaPairRDD<LocalDate, String> commits      = rdd.mapToPair(new CsvParser());

        commits.cache();    //we use it later on to find the winner

        //Aggregate to have one entity per date (not per DOW)
        final JavaPairRDD<LocalDate, Long>           commitsCount           = commits.mapToPair(t -> new Tuple2<>(t._1(), 1L));
        final JavaPairRDD<LocalDate, Long>           dailyCommitsCountByDay = commitsCount.aggregateByKey(0L, (a, b) -> a + b, (a, b) -> a + b);

        dailyCommitsCountByDay.cache();     //we use it later to find anomalous

        //[1]
        final JavaPairRDD<DayOfWeek, Long> dailyCommitCountByDOW  = dailyCommitsCountByDay.mapToPair(new MapDateToDayOfWeek());  //we are still with entity per date we only hold the int for DOW as key so many items here will have the same key
        final Dataset<Row> dataset = calculateCommitCountAggregations(sparkContext, dailyCommitCountByDOW);
        final List<Row> rows = dataset.collectAsList();     //a row per day-of-week

        final EnumMap<DayOfWeek, Double> avgMap    = prepareMap(rows, "AVG"   );
        final EnumMap<DayOfWeek, Double> stdDevMap = prepareMap(rows, "STDDEV");
        final EnumMap<DayOfWeek, Double> pctMap    = prepareMap(rows, "PCT95" );

        //[2]
        final JavaPairRDD<LocalDate, Long> anomalous = findAnomalous(dailyCommitsCountByDay, avgMap, stdDevMap);

        //[3]
        final Tuple2<LocalDate, Long>      crazyDay  = findCrazyDay (anomalous);
        final Tuple2<String, Long>         theWinner = findTheWinner(commits, crazyDay);

        //Print the result
        System.out.println(BAR + "\nAverage\n" +
                avgMap.keySet().stream().map(
                        key -> "\t" + key + " = " + avgMap.get(key)).collect(Collectors.joining("\n")));

        System.out.println(BAR + "\nStdDev\n" +
                avgMap.keySet().stream().map(
                        key -> "\t" + key + " = " + stdDevMap.get(key)).collect(Collectors.joining("\n")));

        System.out.println(BAR + "\nPct95\n" +
                avgMap.keySet().stream().map(
                        key -> "\t" + key + " = " + pctMap.get(key)).collect(Collectors.joining("\n")));

        System.out.println(BAR + "\nAnomalous ["+anomalous.count()+"]\n" +
                anomalous.collect().stream().map(
                        t -> "\t" + t._1().toString()).collect(Collectors.joining("\n")));

        if (crazyDay==null) {
            System.out.println("No Crazy Day... we are all sane here");
        }
        else {
            System.out.println("CRAZY DAY = " + crazyDay);
        }

        if (theWinner==null) {
            System.out.println("No specific winner... Great team work!");
        }
        else {
            System.out.println("And the winner is.... " + theWinner);
        }

        final LocalDate dateCrazy = crazyDay._1();
        final JavaPairRDD<LocalDate, String> commitsOnCrazyDay = commits.filter(x -> x._1().equals(dateCrazy));
        final JavaPairRDD<LocalDate, String> winnerCommits = commitsOnCrazyDay.filter(x -> x._2().equals(theWinner._1()));

        System.out.println(BAR + "\nWinnerCommits ["+winnerCommits.count()+"]\n" +
                winnerCommits.collect().stream()
                        .map(Tuple2::_1).sorted()
                        .map(d -> "\t" + d)
                        .collect(Collectors.joining("\n")));




        sparkContext.close();
    }

    private static String getFileName(String[] args) {
        String fileName;
        if (args==null || args.length==0 || args[0]==null || args[0].length()==0) {
            fileName = FILENAME;
        }
        else {
            fileName = args[0];
        }
        return fileName;
    }

    private static EnumMap<DayOfWeek, Double> prepareMap(List<Row> rows, String columnName) {
        final EnumMap<DayOfWeek, Double> result = new EnumMap<>(DayOfWeek.class);
        for (Row row : rows) {
            final Integer key = row.getAs("dow");
            final Double value = row.getAs(columnName);
            result.put(DayOfWeek.of(key), value);
        }
        return result;
    }

    private static JavaSparkContext createSparkContext() {
        return new JavaSparkContext(
                new SparkConf()
                        .set("spark.executor.memory", "4g")
                        .set("spark.driver.memory", "4g")
                        .setAppName("SparkProcessGithubCommits")
                        .setMaster("local[*]"));
    }

    /**
     * Calculate the average, standard deviation and 95th percentile of the number of commits per day-of-week
     * @return a table with a row for each day-of-week and with columns {dow,AVG,STDDEV,PCT95}
     */
    private static Dataset<Row> calculateCommitCountAggregations(JavaSparkContext sparkContext, JavaPairRDD<DayOfWeek, Long> dailyCommitCountByDOW) {
        final SQLContext sqlContext = new SQLContext(sparkContext);
        StructType schema = createSchema();
        //todo: consider the use of partitionBy(new PartitionByDayOfWeek());
        final JavaRDD<Row> rows = dailyCommitCountByDOW.map(t -> RowFactory.create(t._1().getValue(), t._2()));
        final Dataset<Row> df = sqlContext.createDataFrame(rows, schema);
        df.registerTempTable("COMMITS_COUNT_DOW");
        return sqlContext.sql("SELECT dow, avg(commits) AVG, stddev(commits) STDDEV, percentile(commits, 0.95) PCT95 FROM COMMITS_COUNT_DOW GROUP BY dow");
    }

    private static StructType createSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("dow", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("commits", DataTypes.LongType, true));
        return DataTypes.createStructType(fields);
    }

    /**
     * Find anomalous days, these are days where the number of commits is higher by at least two standard deviations from the average
     * @return pair-rdd with only the anomalous dates. { key => date   value => commits count for this date}
     */
    private static JavaPairRDD<LocalDate, Long> findAnomalous(JavaPairRDD<LocalDate, Long> dailyCommitsCountByDay, EnumMap<DayOfWeek, Double> avgMap, EnumMap<DayOfWeek, Double> stdDevMap) {
        return dailyCommitsCountByDay.filter(new AnomalousFilter(avgMap, stdDevMap));
    }

    /**
     * CrazyDay is the day where we had the maximum commits
     * We look for the max among the anomalous
     */
    private static Tuple2<LocalDate, Long> findCrazyDay(JavaPairRDD<LocalDate, Long> anomalous) {
        if (anomalous.isEmpty()) {
            return null;
        }
        return anomalous.max(new CommitCountComparator<>());
    }

    /**
     * The winner is the author with the most commits on the CrazyDay
     * From all authors committing on the CrazyDay we look for the most influencing one
     * @return the name of the winner and how many commits he pushed on the CrazyDay
     */
    private static Tuple2<String, Long> findTheWinner(JavaPairRDD<LocalDate, String> commits, Tuple2<LocalDate, Long> crazyDay) {
        if (crazyDay==null) return null;

        final LocalDate dateCrazy = crazyDay._1();
        final JavaPairRDD<LocalDate, String> commitsOnCrazyDay = commits.filter(x -> x._1().equals(dateCrazy));
        final JavaPairRDD<String, Long> userCommits = commitsOnCrazyDay.mapToPair(t -> new Tuple2<>(t._2(), 1L));
        final JavaPairRDD<String, Long> commitsCountByUser = userCommits.aggregateByKey(0L, (a, b) -> a + b, (a, b) -> a + b);
        return commitsCountByUser.max(new CommitCountComparator<>());
    }


}
