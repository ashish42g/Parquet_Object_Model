package com.apple.parquet.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.io.IOException;

public class ParquetMapper extends Mapper<LongWritable, SimpleGroup, Text, NullWritable> {
    public void map(LongWritable key, SimpleGroup value, Context context) throws IOException, InterruptedException {
        context.write(new Text(value.getString("OS_VERSION", 0) + "\t" + value.getLong("PRS_ID", 0)), NullWritable.get());
    }
}