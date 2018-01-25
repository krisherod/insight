package org.myorg.quickstart;


//import jdk.nashorn.internal.parser.JSONParser;


import com.datastax.driver.core.Cluster;
//import javafx.event.Event;
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
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
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
//import org.graalvm.compiler.lir.LIRInstruction;
//import scala.util.parsing.json.JSONObject;


import javax.print.attribute.standard.Severity;
import javax.xml.crypto.Data;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.myorg.quickstart.HelperFunctions;




public class StationStreamProcessor {



    public static HashMap<Integer, HashMap<Integer, Float>> stations = new HashMap<Integer, HashMap<Integer, Float>>();
    public static HashMap<Integer, Integer> station_pairs = new HashMap<Integer, Integer>();


    public static void main(String[] args) throws Exception {

        // hash map with all of the values



        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String prefix = parameterTool.get("prefix", "PREFIX:");

        // get data from kafka consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ec2-34-213-63-132.us-west-2.compute.amazonaws.com:9092");
        properties.setProperty("group.id", "asdf");

        // create the consumer
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>(
                "station-topic",
                new SimpleStringSchema(), properties);


        myConsumer.setStartFromLatest();

        // split incoming string into a list
        DataStream<Tuple6<Integer, Integer, Float, Long, String, String>> stream = env
                .addSource(myConsumer)
                .map(new PrefixingMapper());


        DataStream<Tuple6<Integer, Integer, Float, Long, String, String>> withTimestampsAndWatermarks = stream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());


        DataStream<String> average = withTimestampsAndWatermarks
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(5)))
                .aggregate(new AverageAggregate());




        Pattern<Tuple6<Integer, Integer, Float, Long, String, String>, ?> warningPattern = Pattern.<Tuple6<Integer, Integer, Float, Long, String, String>>begin("first")
                .where(new SimpleCondition<Tuple6<Integer, Integer, Float, Long, String, String>>() {
                    @Override
                    public boolean filter(Tuple6<Integer, Integer, Float, Long, String, String> value1) throws Exception {
                        return value1.f2 > 5000;
                    }
                })
                .next("second")
                .where(new SimpleCondition<Tuple6<Integer, Integer, Float, Long, String, String>>() {
                    @Override
                    public boolean filter(Tuple6<Integer, Integer, Float, Long, String, String> value2) throws Exception {
                        return value2.f2 > 5000;
                    }
                }).within(Time.seconds(10));

        // Create a pattern stream from our warning pattern
        PatternStream<Tuple6<Integer, Integer, Float, Long, String, String>> tempPatternStream = CEP.pattern(
                stream.keyBy(0),
                warningPattern);

        DataStream<Tuple6<Integer, Integer, Float, Long, String, String>> warnings = tempPatternStream.select(new PatternSelectFunction<Tuple6<Integer, Integer, Float, Long, String, String>, Tuple6<Integer, Integer, Float, Long, String, String>>() {
            @Override
            public Tuple6<Integer, Integer, Float, Long, String, String> select(Map<String, List<Tuple6<Integer, Integer, Float, Long, String, String>>> warningPattern) throws Exception {
//                System.out.println(warningPattern.get("second"));
                Tuple6<Integer, Integer, Float, Long, String, String> g = warningPattern.get("second").get(0);
                return g;

            }
        });






        // check if there are consecutive warnings
        Pattern<Tuple6<Integer, Integer, Float, Long, String, String>, ?> alertPattern = Pattern.<Tuple6<Integer, Integer, Float, Long, String, String>>begin("first")
                .where(new SimpleCondition<Tuple6<Integer, Integer, Float, Long, String, String>>() {
                    @Override
                    public boolean filter(Tuple6<Integer, Integer, Float, Long, String, String> value1) throws Exception {
                        station_pairs.put(value1.f1, value1.f0);
                        return value1.f2 > 5000;

                    }
                })
                .next("second")
                .where(new SimpleCondition<Tuple6<Integer, Integer, Float, Long, String, String>>() {
                    @Override
                    public boolean filter(Tuple6<Integer, Integer, Float, Long, String, String> value2) throws Exception {
                        int g;
                        int t = station_pairs.get(value2.f1);


                        if (value2.f0 != t) {
                            return value2.f2 > 5000;
                        } else {
                            return false;
                        }

                    }
                }).within(Time.seconds(10));

        // Create a pattern stream from our warning pattern
        PatternStream<Tuple6<Integer, Integer, Float, Long, String, String>> alertPatternStream = CEP.pattern(
                warnings.keyBy(1),
                alertPattern);

        DataStream<String> alerts = alertPatternStream.select(new PatternSelectFunction<Tuple6<Integer, Integer, Float, Long, String, String>, String>() {
            @Override
            public String select(Map<String, List<Tuple6<Integer, Integer, Float, Long, String, String>>> warningPattern) throws Exception {
//                System.out.println(warningPattern.get("second"));
                Tuple6<Integer, Integer, Float, Long, String, String> g = warningPattern.get("second").get(0);
                return String.format("%d,%d,%f,%s,%s,0,1,0", g.f0, g.f1, g.f2, g.f3, g.f4);

            }
        });








        Pattern<Tuple6<Integer, Integer, Float, Long, String, String>, ?> brokenPattern = Pattern.<Tuple6<Integer, Integer, Float, Long, String, String>>begin("first")
                .times(10)
                .where(new IterativeCondition<Tuple6<Integer, Integer, Float, Long, String, String>>() {
                    @Override

                    public boolean filter(Tuple6<Integer, Integer, Float, Long, String, String> value1, Context<Tuple6<Integer, Integer, Float, Long, String, String>> context) throws Exception {
                        if (stations.containsKey(value1.f1)) {
                            if (stations.get(value1.f1).containsKey(value1.f0)) {
                                return value1.f2 > stations.get(value1.f1).get(value1.f0)*10 || value1.f2 < 0;
                            } else {
                                return false;
                            }
                        } else {
                            return false;
                        }

                    }
                }).within(Time.seconds(30));

        // Create a pattern stream from our warning pattern
        PatternStream<Tuple6<Integer, Integer, Float, Long, String, String>> brokenPatternStream = CEP.pattern(
                stream.keyBy(0),
                brokenPattern);

        DataStream<String> brokenSensors = brokenPatternStream.select(new PatternSelectFunction<Tuple6<Integer, Integer, Float, Long, String, String>, String>() {
            @Override
            public String select(Map<String, List<Tuple6<Integer, Integer, Float, Long, String, String>>> warningPattern) throws Exception {
                Tuple6<Integer, Integer, Float, Long, String, String> g = warningPattern.get("first").get(0);
                return String.format("%d,%d,%f,%s,%s,0,0,1", g.f0, g.f1, g.f2, g.f3, g.f4);

            }
        });







//        withTimestampsAndWatermarks.print();
//        alerts.print();
//        brokenSensors.print();


//        warnings.print();
//        alerts.print();
        brokenSensors.print();
//        withTimestampsAndWatermarks.print();

        DataStreamSink<String> alertPatternSink = alerts.addSink(
                new FlinkKafkaProducer010<String>(
                        "my-topic3",
                        new SimpleStringSchema(), properties
                )
        );
        alertPatternSink.name("warning pattern sink");


        DataStreamSink<String> brokenPatternSink = brokenSensors.addSink(
                new FlinkKafkaProducer010<String>(
                        "my-topic3",
                        new SimpleStringSchema(), properties
                )
        );
        brokenPatternSink.name("broken pattern sink");


        // schema: station_id, group_id, value, lat, long
        DataStreamSink<String> averageSink = average.addSink(
                new FlinkKafkaProducer010<String>(
                        "average-topic",
                        new SimpleStringSchema(), properties
                )
        );
        alertPatternSink.name("average value sink");

        average.print();




        env.execute("");

    }

    //
    // 	User Functions
    //

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */

    // this class will split my string into an array
    private static class PrefixingMapper implements MapFunction<String, Tuple6<Integer, Integer, Float, Long, String, String>> {
        //        private final String prefix;
        @Override
        public Tuple6<Integer, Integer, Float, Long, String, String> map(String prefix) {

            List<String> items = Arrays.asList(prefix.split("\t"));

            return new Tuple6<Integer, Integer, Float, Long, String, String>(Integer.valueOf(items.get(0)), Integer.valueOf(items.get(1)), Float.valueOf(items.get(3)), Long.valueOf(items.get(2))*1000, items.get(4), items.get(5));

        }
    }



    private static class TupleToString implements MapFunction<Tuple6<String, String, Float, Long, String, String>, String> {
        @Override
        public String map(Tuple6<String, String, Float, Long, String, String> value) throws Exception {
            return value.f0 + "" + value.f1 + "" + String.valueOf(value.f2) + "" + value.f3 + "" + value.f4 + "" + value.f5;
        }
    }





    private static class AverageAggregate implements AggregateFunction<Tuple6<Integer, Integer, Float, Long, String, String>, Tuple6<Integer, Integer, Float, Float, String, String>, String> {

        //
//        public static Map<Integer, Float> stationAverages = new HashMap<Integer, Float>();

        public static Float key = new Float(0.0);


        @Override
        public Tuple6<Integer, Integer, Float, Float, String, String> createAccumulator() {
            return new Tuple6<Integer, Integer, Float, Float, String, String>(0, 0, new Float(0), new Float(0), "", "");
        }

        @Override
        public Tuple6<Integer, Integer, Float, Float, String, String> add(Tuple6<Integer, Integer, Float, Long, String, String> value, Tuple6<Integer, Integer, Float, Float, String, String> accumulator) {

            // compare current sensor against previous value
            List<String> items = Arrays.asList(value.f5.split(","));


            // return the average of the last n minutes of data
            return new Tuple6<Integer, Integer, Float, Float, String, String>(value.f0, value.f1, accumulator.f1 + value.f2, accumulator.f2 + new Float(1.0), value.f4, value.f5);
        }

        @Override
        public String getResult(final Tuple6<Integer, Integer, Float, Float, String, String> accumulator) {
            //accumulator.f1, (accumulator.f2+accumulator.f3)/2)
            stations.put(accumulator.f1, new HashMap<Integer, Float>(){{put(accumulator.f0, accumulator.f2);}});
            //return new Tuple2<Integer, Float>(accumulator.f0, key);
            return accumulator.f0 + "," + accumulator.f1 + "," + String.valueOf(accumulator.f2) + "," + accumulator.f4;
        }

        @Override
        public Tuple6<Integer, Integer, Float, Float, String, String> merge(Tuple6<Integer, Integer, Float, Float, String, String> a, Tuple6<Integer, Integer, Float, Float, String, String> b) {
            return new Tuple6<Integer, Integer, Float, Float, String, String>(a.f0, a.f1, a.f2 + b.f2, a.f3 + b.f3, a.f4, a.f5);
        }
    }




    public static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Tuple6<Integer, Integer, Float, Long, String, String>> {

        private final long maxOutOfOrderness = 10000; // 3.5 seconds

        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Tuple6<Integer, Integer, Float, Long, String, String> element, long previousElementTimestamp) {
            long timestamp = element.f3;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current highest timestamp minus the out-of-orderness bound
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }


}
