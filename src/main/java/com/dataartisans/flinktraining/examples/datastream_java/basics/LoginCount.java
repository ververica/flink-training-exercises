package com.dataartisans.flinktraining.examples.datastream_java.basics;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.CoreEvent;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CoreEventSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Example that counts the rides for each driver.
 *
 * Parameters:
 *   -input path-to-input-file
 *
 * 	Note that this is implicitly keeping state for each driver.
 * 	This sort of simple, non-windowed aggregation on an unbounded set of keys will use an unbounded amount of state.
 * 	When this is an issue, look at the SQL/Table API, or ProcessFunction, or state TTL, all of which provide
 * 	mechanisms for expiring state for stale keys.
 */
public class LoginCount {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToEventData);

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served every second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<CoreEvent> rides = env.addSource(new CoreEventSource(input, maxEventDelay, servingSpeedFactor));

        // map each ride to a tuple of (driverId, 1)
        DataStream<Tuple4<String, String, String, Long>> tuples = rides.map(new MapFunction<CoreEvent, Tuple4<String, String, String, Long>>() {
            @Override
            public Tuple4<String, String, String, Long> map(CoreEvent event) throws Exception {
                return new Tuple4<String, String, String, Long>(event.getProperties().get("userId"),
                        event.getProperties().get("domainCname"), event.getProperties().get("authenticationMethod"), 1L) ;
            }
        });

        // partition the stream by the driverId
        KeyedStream<Tuple4<String, String, String, Long>, Tuple> keyedByUserId = tuples.keyBy(0, 1, 2);

        // count the rides for each driver
        DataStream<Tuple4<String, String, String, Long>> loginCounts = tuples.filter(new FilterFunction<Tuple4<String, String, String, Long>>() {
            @Override
            public boolean filter(Tuple4<String, String, String, Long> t) throws Exception {
                return "WEBSITE".equals(t.f2);
            }
        }).keyBy(0, 1, 2).sum(3);

        FilterFunction<Tuple4<String, String, String, Long>> tFilterFunction = new FilterFunction<Tuple4<String, String, String, Long>>() {
            @Override
            public boolean filter(Tuple4 t) throws Exception {
                return "API".equals(t.f2);
            }
        };

        DataStream<Tuple4<String, String, String, Long>> accessCounts = keyedByUserId.timeWindow(Time.minutes(15)).apply(new WindowFunction<Tuple4<String, String, String, Long>, Tuple4<String, String, String, Long>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<String, String, String, Long>> iterable, Collector<Tuple4<String, String, String, Long>> collector) throws Exception {
                HashSet<Tuple4<String, String, String, Long>> uniqueUserDomain = new HashSet<Tuple4<String, String, String, Long>>();
                for(Tuple4 element: iterable) {
                    uniqueUserDomain.add(element);
                }

                uniqueUserDomain.forEach(new Consumer<Tuple4<String, String, String, Long>>() {
                    @Override
                    public void accept(Tuple4<String, String, String, Long> unique) {
                       collector.collect(unique);
                    }
                });
            }
        }).filter(tFilterFunction);

        // we could, in fact, print out any or all of these streams
        loginCounts.print();

        accessCounts.print();

        // run the cleansing pipeline
        env.execute("Login Count");
    }
}
