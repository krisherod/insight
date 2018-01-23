package org.myorg.quickstart;


//import jdk.nashorn.internal.parser.JSONParser;


import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;

import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraTupleSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.metrics.stats.Count;
//import scala.util.parsing.json.JSONObject;


import javax.print.attribute.standard.Severity;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.myorg.quickstart.HelperFunctions;


public class StationStreamProcessor {

    public static void main(String[] args) throws Exception {

        // hash map with the rolling averages of all the stations
        final Map<String, String> stationAverages = new HashMap<>();


        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);


        String prefix = parameterTool.get("prefix", "PREFIX:");

        // get data from kafka consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ec2-34-213-63-132.us-west-2.compute.amazonaws.com:9092");
        properties.setProperty("group.id", "asdf");


        // create the consumer
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>(
                "my-topic2",
                new SimpleStringSchema(), properties);


        // set the configuration
        myConsumer.setStartFromEarliest();
        myConsumer.setStartFromLatest();
        myConsumer.setStartFromGroupOffsets();


        // split incoming string into a list
        DataStream<Tuple6<String, String, Float, String, String, String>> stream = env
                .addSource(myConsumer)
                .map(new HelperFunctions.PrefixingMapper());




        // pattern for detecting when there is an alert
        final Pattern<Tuple6<String, String, Float, String, String, String>, ?> warning = Pattern.<Tuple6<String, String, Float, String, String, String>> begin("first Event")
                .where(new SimpleCondition<Tuple6<String, String, Float, String, String, String>>() {
                           @Override
                           public boolean filter(Tuple6<String, String, Float, String, String, String> event1) throws Exception {
                               stationAverages.put(event1.f1, event1.f0);
                               return event1.f2 > 10000;
                           }
                       }
                ).followedBy("second Event")
                .where(new SimpleCondition<Tuple6<String, String, Float, String, String, String>>() {
                    @Override
                    public boolean filter(Tuple6<String, String, Float, String, String, String> event2) throws Exception {
                        return stationAverages.get(event2.f0) != event2.f1;
                    }
                }).next("third Event")
                .where(new SimpleCondition<Tuple6<String, String, Float, String, String, String>>() {
                    @Override
                    public boolean filter(Tuple6<String, String, Float, String, String, String> event3) throws Exception {
                        return event3.f2 > 10000;
                    }
                }).next("fourth Event")
                .where(new SimpleCondition<Tuple6<String, String, Float, String, String, String>>() {
                    @Override
                    public boolean filter(Tuple6<String, String, Float, String, String, String> event3) throws Exception {
                        return event3.f2 > 10000;
                    }
                }).next("fifth Event")
                .where(new SimpleCondition<Tuple6<String, String, Float, String, String, String>>() {
                    @Override
                    public boolean filter(Tuple6<String, String, Float, String, String, String> event3) throws Exception {
                        return event3.f2 > 10000;
                    }
                }).next("sixth Event")
                .where(new SimpleCondition<Tuple6<String, String, Float, String, String, String>>() {
                    @Override
                    public boolean filter(Tuple6<String, String, Float, String, String, String> event3) throws Exception {
                        return event3.f2 > 10000;
                    }
                }).within(Time.seconds(300));




        // Pattern stream for a warning
        PatternStream<Tuple6<String, String, Float, String, String, String>> warningPatternStream = CEP.pattern(
                stream.keyBy(0),
                warning
        );




        DataStream<String> stream2 = stream
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(10)))
                .aggregate(new AverageAggregate());





        // schema: station_id, group_id, concentration, lat, long, warning, alert, status
        DataStream<String> warningStream = warningPatternStream.select(new PatternSelectFunction<Tuple6<String, String, Float, String, String, String>, String>() {
            @Override
            public String select(Map<String, List<Tuple6<String, String, Float, String, String, String>>> warningPattern) throws Exception {
                System.out.println(warningPattern.get("first Event"));
                Tuple6<String, String, Float, String, String, String> g = warningPattern.get("first Event").get(0);
                return String.format("%s,%s,%f,%s,%s,1,0,0", g.f0, g.f1, g.f2, g.f3, g.f4);

            }
        });




        warningStream.print();

        // Produce the warnings
        DataStreamSink<String> warningPatternSink = warningStream.addSink(
            new FlinkKafkaProducer010<String>(
                    "my-topic3",
                    new SimpleStringSchema(), properties
            )
        );
        warningPatternSink.name("warning pattern sink");

        // Produce the averages. Mostly for testing right now
        DataStreamSink<String> stationAverageSink = stream2.addSink(
                new FlinkKafkaProducer010<String>(
                        "my-topic4",
                        new SimpleStringSchema(), properties
                )
        );
        stationAverageSink.name("station average sink");




        env.execute("");

    }


    // get the rolling average of the surrounding sensors
    private static class AverageAggregate implements AggregateFunction<Tuple6<String, String, Float, String, String, String>, Tuple3<String, Float, Float>, String> {

        //
        public static Map<String, Float> stationAverages = new HashMap<String, Float>();

        public static Float key = new Float(0.0);


        @Override
        public Tuple3<String, Float, Float> createAccumulator() {
            return new Tuple3<String, Float, Float>("", new Float(0), new Float(0));
        }

        @Override
        public Tuple3<String, Float, Float> add(Tuple6<String, String, Float, String, String, String> value, Tuple3<String, Float, Float> accumulator) {

            // compare current sensor against previous value
            List<String> items = Arrays.asList(value.f5.split(","));

            for (String number : items) {

                // check nearest sensors
                number = number.replace("\n", "");

                if (stationAverages.containsKey(number) && Math.abs(stationAverages.get(number)-value.f2) > 10) {
                    // do something if current value is different from averages
                    key += new Float(1);
                }
            }

            // return the average of the last n minutes of data
            return new Tuple3<String, Float, Float>(value.f0, accumulator.f1 + value.f2, accumulator.f2 + new Float(1.0));
        }

        @Override
        public String getResult(Tuple3<String, Float, Float> accumulator) {
            stationAverages.put(accumulator.f0, accumulator.f1/accumulator.f2);
//            return new Tuple2<String, Float>(accumulator.f0, key);
            return new String(accumulator.f0 + "," + accumulator.f1/accumulator.f2);
        }

        @Override
        public Tuple3<String, Float, Float> merge(Tuple3<String, Float, Float> a, Tuple3<String, Float, Float> b) {
            return new Tuple3<String, Float, Float>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }



}

