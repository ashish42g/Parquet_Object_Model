package com.apple.parquet.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ParquetReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String split_val[] = key.toString().split("\t");
        if (split_val.length > 1) {
            String dim_code = split_val[0];
            context.getCounter("COUNT", dim_code).increment(1L);
        }
    }
}
