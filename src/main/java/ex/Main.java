package ex;

import ex.deserialization.FlightParser;
import ex.deserialization.FlightParserImpl;
import ex.deserialization.objects.Flight;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

public class Main {

    private static final String DEPARTURES = "./Fraport/*DEPARTURES*";
    private static final String ARRIVALS = "./Fraport/*ARRIVALS*";
    private static final String ALL_FLIGHTS = "./Fraport/";

    public static void main(String... args) {
        // comment out to enable log messages
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        var airportInfo = new AirportInfoImpl();
        var flightParser = new FlightParserImpl();

        exampleOutput(airportInfo, flightParser);
    }

    private static void exampleOutput(AirportInfoImpl airportInfo, FlightParser flightParser) {
        var firstDay = flightParser.parseRows("./Fraport/*-08-08-*");
        airportInfo.sparkExample(firstDay);
    }
}
