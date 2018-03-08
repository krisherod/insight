package org.myorg.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

import java.util.Arrays;
import java.util.List;

public class HelperFunctions {

    /**
     * Takes a string from methane data kafka consumer and turns it into a tuple
     * * @param Tuple6<Integer, Integer, Float, Long, String, String> The incoming methane sensor data
     * @return Tuple6<Integer, Integer, Float, Long, String, String> This returns a split version of the string
     */
    public static class ParseSensorData implements
            MapFunction<String, Tuple6<Integer, Integer, Float, Long, String, String>> {

        @Override
        public Tuple6<Integer, Integer, Float, Long, String, String> map(String sensorData) {

            List<String> items = Arrays.asList(sensorData.split("\t"));
            Float concentration = Float.valueOf(items.get(3));

            try {
                return new Tuple6<Integer, Integer, Float, Long, String, String>
                        (Integer.valueOf(items.get(0)), Integer.valueOf(items.get(1)),
                                concentration, Long.valueOf(items.get(2))*1000, items.get(4), items.get(5));
            } catch (Exception e) {
                return null;
            }



        }
    }


    /**
     * Takes a string from methane data kafka consumer and turns it into a tuple
     * @param Tuple5<Integer, Integer, Float, Long, String> The incoming temperature sensor data
     * @return Tuple6<Integer, Integer, Float, Long, String, String> This returns a split version of the string
     */
    public static class ParseTemperatureData implements
            MapFunction<String, Tuple5<Integer, Integer, Float, Long, String>> {

        @Override
        public Tuple5<Integer, Integer, Float, Long, String> map(String sensorData) {

            List<String> items = Arrays.asList(sensorData.split("\t"));
            Float concentration = Float.valueOf(items.get(3));

            return new Tuple5<Integer, Integer, Float, Long, String>(Integer.valueOf(items.get(0)),
                    Integer.valueOf(items.get(1)), concentration, Long.valueOf(items.get(2))*1000, items.get(4));

        }
    }


}



