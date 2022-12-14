package ex;

import ex.deserialization.objects.Flight;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

public class AirportInfoImpl implements AirportInfo {

    /**
     * Usage of some example operations.
     *
     * @param flights dataframe of flights
     */
    public void sparkExample(Dataset<Row> flights) {
        System.out.println("Example printSchema");
        flights.printSchema();

        System.out.println("Example select and filter");
        // operations on a dataframe or rdd do not modify it, but return a new one
        var selectAirlineDisplayCode = flights.select("flight.operatingAirline.iataCode", "flight.aircraftType.icaoCode").filter((FilterFunction<Row>) r -> !r.anyNull());
        selectAirlineDisplayCode.show(false);

        System.out.println("Example groupBy and count");
        var countOfAirlines = selectAirlineDisplayCode.groupBy("iataCode").count();
        countOfAirlines.show(false);

        System.out.println("Example where");
        var selectOnlyLufthansa = selectAirlineDisplayCode.where("iataCode = 'LH'");
        selectOnlyLufthansa.show(false);

        System.out.println("Example map to String");
        var onlyAircraftIcaoCodeAsString = selectOnlyLufthansa.map((MapFunction<Row, String>) r -> r.getString(1), Encoders.STRING());
        onlyAircraftIcaoCodeAsString.show(false);

        System.out.println("Example mapToPair and reduceByKey");
        var rdd = selectOnlyLufthansa.toJavaRDD();
        var paired = rdd.mapToPair(r -> Tuple2.apply(r.getString(1), 1L));
        var reducedByKey = paired.reduceByKey((a, b) -> a + b);
        reducedByKey.take(20).forEach(t -> System.out.println(t._1() + " " + t._2()));
    }

    /**
     * Task 1
     * Return a dataframe, in which every row contains the destination airport and its count over all available data
     * of departing flights, sorted by count in descending order.
     * The column names are (in this order):
     * arrivalAirport | count
     * Remove entries that do not contain an arrivalAirport member (null or empty).
     * Use the given Dataframe.
     *
     * @param departingFlights Dataframe containing the rows of departing flights
     * @return a dataframe containing statistics of the most common destinations
     */
    @Override
    public Dataset<Row> mostCommonDestinations(Dataset<Row> departingFlights) {
        var flights =
                departingFlights.select("flight.arrivalAirport")
                        .filter(
                                col("arrivalAirport").isNotNull()
                                        .and(col("arrivalAirport").notEqual("")))
                        .groupBy("arrivalAirport").count()
                        .sort(col("count").desc());

        return flights;
    }

    /**
     * Task 2
     * Return a dataframe, in which every row contains a gate and the amount at which that gate was used for flights to
     * Berlin ("count"), sorted by count in descending order. Do not include gates with 0 flights to Berlin.
     * The column names are (in this order):
     * gate | count
     * Remove entries that do not contain a gate member (null or empty).
     * Use the given Dataframe.
     *
     * @param departureFlights Dataframe containing the rows of departing flights
     * @return dataframe with statistics about flights to Berlin per gate
     */
    @Override
    public Dataset<Row> gatesWithFlightsToBerlin(Dataset<Row> departureFlights) {
        var flights =
                departureFlights.select("flight.departure.gates.gate")
                        .where(col("flight.arrivalAirport").$eq$eq$eq("BER"))
                        .filter(col("gate").isNotNull())
                        .groupBy("gate").count()
                        .sort(col("count").desc()
                        );


        flights.show(false);
        return flights;
    }

    /**
     * Task 3
     * Return a JavaPairRDD with String keys and Long values, containing count of flights per aircraft on the given
     * originDate. The String keys are the modelNames of each aircraft and their Long value is the amount of flights for
     * that modelName at the given originDate. Do not include aircrafts with 0 flights.
     * Remove entries that do not contain a modelName member (null or empty).
     * The date string is of the form 'YYYY-MM-DD'.
     * Use the given dataframe.
     *
     * @param flights    Dataframe containing the rows of flights
     * @param originDate the date to find the most used aircraft for
     * @return tuple containing the modelName of the aircraft and its total number
     */
    @Override
    public JavaPairRDD<String, Long> aircraftCountOnDate(Dataset<Row> flights, String originDate) {

        var flight = flights.select("flight.aircraftType.modelName")
                .where(col("flight.originDate").$eq$eq$eq(originDate))
                .filter(col("modelName").isNotNull())
                .filter(col("modelName").notEqual(""))
                .groupBy("modelName").count()
                .sort(col("count").desc());

        JavaPairRDD<String, Long> flight_javaPairRDD = flight.toJavaRDD().mapToPair(row -> {
            return new Tuple2<String, Long>(row.getString(0), row.getLong(1));
        });

        flight.show(false);
        flight_javaPairRDD.foreach(data -> {
            System.out.println("Model: "+data._1+"\nNumber of flights: "+data._2);
        } );
        return flight_javaPairRDD;
    }

    /**
     * Task 4
     * Returns the date string of the day at which Ryanair had a strike in the given Dataframe.
     * The returned string is of the form 'YYYY-MM-DD'.
     * Hint: There were strikes at two days in the given period of time. Both are accepted as valid results.
     *
     * @param flights Dataframe containing the rows of flights
     * @return day of strike
     */
    @Override
    public String ryanairStrike(Dataset<Row> flights) {
        //I assume that input might consist of many days(e.g. in one json file there are different days), so therefore we create the list
        List <String> my_list = new ArrayList<>();

        //C - canceled
        var flight1 = flights.select("flight.originDate")
                .where(col("flight.flightStatus").$eq$eq$eq("C"))
                .where(col("flight.operatingAirline.name").$eq$eq$eq("Lufthansa"));

        my_list = flight1.as(Encoders.STRING()).collectAsList();


        Set<String> my_set1 = new HashSet<>(my_list);
        String str = String.join(",", my_set1);

        System.out.println("my set: "+str);
        flight1.show(false);
        //System.out.println(flight1);
        return null;
    }

    /**
     * Task 5
     * Returns a dataset of Flight objects. The dataset only contains flights of the given airline with at least one
     * of the given status codes. Uses the given Dataset of Flights.
     *
     * @param flights            Dataset containing the Flight objects
     * @param airlineDisplayCode the display code of the airline
     * @param status1            the status code of the flight
     * @param status             more status codes
     * @return dataset of Flight objects matching the required fields
     */
    @Override
    public Dataset<Flight> flightsOfAirlineWithStatus(Dataset<Flight> flights, String airlineDisplayCode, String status1, String... status) {
        List<String> statuses = new ArrayList<>(status.length + 1);
        statuses.addAll(List.of(status));
        statuses.add(status1);
        return flights.filter((FilterFunction<Flight>) value -> value.getAirlineDisplayCode().equals(airlineDisplayCode)
                && statuses.contains(value.getFlightStatus()));
    }

    /**
     * Task 6
     * Returns the average number of flights per day between the given timestamps (both included).
     * The timestamps are of the form 'hh:mm:ss'. Uses the given Dataset of Flights.
     * Hint: You only need to consider "scheduledTime" and "originDate" for this. Do not include flights with
     * empty "scheduledTime" or "originDate" fields. You can assume that lowerLimit is always before or equal
     * to upperLimit.
     *
     * @param flights    Dataset containing the arriving Flight objects
     * @param lowerLimit start timestamp (included)
     * @param upperLimit end timestamp (included)
     * @return average number of flights between the given timestamps (both included)
     */
    @Override
    public double avgNumberOfFlightsInWindow(Dataset<Flight> flights, String lowerLimit, String upperLimit) {
        var average = flights.filter((FilterFunction<Flight>) value ->
                value.getScheduled().length() > 0
                        && value.getOriginDate().length() > 0)
                .filter((FilterFunction<Flight>) value ->
                        isBefore(lowerLimit, getTimeFromDate(value.getScheduled()))
                                && isBefore(getTimeFromDate(value.getScheduled()), upperLimit))
                .groupByKey((MapFunction<Flight, String>) v -> v.getOriginDate(), Encoders.STRING()).count()
                .agg(avg(col("count(1)"))).first().getDouble(0);

        return average;
    }

    /**
     * Returns true if the first timestamp is before or equal to the second timestamp. Both timestamps must match
     * the format 'hh:mm:ss'.
     * You can use this for Task 6. Alternatively use LocalTime or similar API (or your own custom method).
     *
     * @param before the first timestamp
     * @param after  the second timestamp
     * @return true if the first timestamp is before or equal to the second timestamp
     */
    private static boolean isBefore(String before, String after) {
        for (int i = 0; i < before.length(); i++) {
            char bef = before.charAt(i);
            char aft = after.charAt(i);
            if (bef == ':') continue;
            if (bef < aft) return true;
            if (bef > aft) return false;
        }

        return true;
    }

    private static String getTimeFromDate(String value) {
        return value.substring(value.indexOf("T")+1, value.length() - 1);
    }
}
