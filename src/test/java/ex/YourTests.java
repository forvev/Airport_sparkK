package ex;

import ex.deserialization.FlightParser;
import ex.deserialization.FlightParserImpl;
import ex.deserialization.objects.Flight;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.*;

import static ex.PublicTests.FLIGHTS_PATH;
import static org.junit.jupiter.api.Assertions.*;

public class YourTests {

    static AirportInfo uut;
    static FlightParser fput;

    @BeforeAll
    static void init() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        uut = new AirportInfoImpl();
        fput = new FlightParserImpl();
    }

    @Nested
    @DisplayName("Task 5")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class Task5 {

        Dataset<Flight> flights;

        @BeforeAll
        @Timeout(10)
        void setup() {
            flights = fput.parseFlights(FLIGHTS_PATH);
        }

        @Test
        void test1() {
            var res = uut.flightsOfAirlineWithStatus(flights, "doesnotexist", "asdgghh", "asddsf");
            assertEquals(0, res.count());
        }

        @Test
        void test2() {
            var res = uut.flightsOfAirlineWithStatus(flights, "LH", "B", "B");
            assertEquals(126, res.count());
            assertFalse(res.collectAsList().stream().anyMatch(f -> !"LH".equals(f.getAirlineDisplayCode())
                    || !"B".equals(f.getFlightStatus())));
        }
    }

    @Nested
    @DisplayName("Task 6")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class Task6 {

        Dataset<Flight> flights;

        @BeforeAll
        @Timeout(10)
        void setup() {
            flights = fput.parseFlights(FLIGHTS_PATH);
        }

        @Test
        void test1() {
            assertEquals(3230d/3d, uut.avgNumberOfFlightsInWindow(flights, "12:00:00", "23:59:59"));
        }

        @Test
        void test2() {
            assertEquals(154d, uut.avgNumberOfFlightsInWindow(flights, "13:00:00", "14:00:00"));
        }

        @Test
        void test3() {
            assertEquals(73d/3d, uut.avgNumberOfFlightsInWindow(flights, "13:09:59", "13:19:59"));
        }

    }

}
