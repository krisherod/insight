package org.myorg.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;

import java.util.Arrays;
import java.util.List;

public class HelperFunctions {

    //
    // 	User Functions
    //

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */

    // this class will split my string into an array
    public static class PrefixingMapper implements MapFunction<String, Tuple6<String, String, Float, String, String, String>> {
        //        private final String prefix;
        @Override
        public Tuple6<String, String, Float, String, String, String> map(String prefix) {

            List<String> items = Arrays.asList(prefix.split("\t"));
//            Tuple2<String, Float> tup = new Tuple2<String, Float>(items.get(0), new Float(items.get(2)));
            return new Tuple6<String, String, Float, String, String, String>(items.get(0), items.get(1), Float.valueOf(items.get(3)), items.get(2), items.get(4), items.get(5));
//            return new Tuple2<Integer, Integer>(1, 1);

        }
    }



    public static class TupleToString implements MapFunction<Tuple6<Integer, Integer, Float, String, String, String>, String> {
        @Override
        public String map(Tuple6<Integer, Integer, Float, String, String, String> value) throws Exception {
            return value.f0 + "" + value.f1 + "" + String.valueOf(value.f2) + "" + value.f3 + "" + value.f4 + "" + value.f5;
        }
    }



}

