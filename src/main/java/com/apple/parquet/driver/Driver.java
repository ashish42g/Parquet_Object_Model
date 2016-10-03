package com.apple.parquet.driver;

import com.apple.parquet.mapreduce.ParquetMapper;
import com.apple.parquet.mapreduce.ParquetReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.Log;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Iterator;


public class Driver extends Configured implements Tool {
    private static final Log LOG = Log.getLog(Driver.class);

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), new Driver(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            LOG.error("Usage: " + getClass().getName() + " INPUTFILE OUTPUTFILE");
            return 1;
        }

        String inputFile = args[0];
        String outputFile = args[1];
        Path parquetFilePath = null;

        //String compression = (args.length > 2) ? args[2] : "none";

        if (inputFile == null) {
            LOG.error("No file found for ");
            return 1;
        }

        LOG.info("Getting schema from " + parquetFilePath);

        Configuration conf = getConf();
        //RemoteIterator<LocatedFileStatus> it = FileSystem.get(conf).listFiles(parquetFilePath, true);
        FileStatus[] fileStatuses = FileSystem.get(conf).listStatus(new Path(inputFile));


        for (FileStatus fileStatus : fileStatuses) {
            parquetFilePath = fileStatus.getPath();
            break;
        }

        if (parquetFilePath == null) {
            LOG.error("No file found for " + inputFile);
            return 1;
        }

        ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, parquetFilePath);
        MessageType schema = readFooter.getFileMetaData().getSchema();
        GroupWriteSupport.setSchema(schema, conf);

        LOG.info(schema);

        Job job = Job.getInstance();
        job.setJarByClass(getClass());
        job.setInputFormatClass(ExampleInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        job.setMapperClass(ParquetMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(ParquetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.waitForCompletion(true);


        FileSystem fileSystem1 = FileSystem.get(conf);
        Path outFilePath1 = new Path(outputFile + "/" + "part-r-00000.txt");
        FSDataOutputStream dataOutputStream1 = fileSystem1.create(outFilePath1, true);
        if (outFilePath1 != null) {
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream1));
            Iterator it = job.getCounters().getGroup("COUNT").iterator();

            while (it.hasNext()) {
                Counter c = (Counter) it.next();
                String dim_code = c.getName();

                bufferedWriter.write(dim_code + "\t" + c.getValue());
                bufferedWriter.newLine();
            }

            bufferedWriter.flush();
            bufferedWriter.close();
        }
        return 0;
    }
}
