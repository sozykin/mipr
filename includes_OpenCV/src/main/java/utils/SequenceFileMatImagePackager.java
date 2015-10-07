package utils;

import core.MiprConfigurationParser;
import opencv.CombineMatImageInputFormat;
import opencv.MatImageWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by Epanchee on 26.05.2015.
 */
public class SequenceFileMatImagePackager {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = args[0];
        String output = args[1];

        Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new MiprConfigurationParser().getOpenCVUri(), conf);
        conf.set("mapreduce.map.memory.mb", "1250");
        conf.set("mapreduce.reduce.memory.mb", "1250");
        Job job = new Job(conf);
        job.setJarByClass(SequenceFileMatImagePackager.class);
        job.setMapperClass(SequenceFileMatImagePackagerMapper.class);
        job.setReducerClass(SequenceFileMatImagePackagerReducer.class);
        job.setNumReduceTasks(3); // count of resulted seq files
        job.setInputFormatClass(CombineMatImageInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class); // that is all
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MatImageWritable.class);
        outputPath.getFileSystem(conf).delete(outputPath, true); // delete folder if exists

        job.waitForCompletion(true);

    }

    static class SequenceFileMatImagePackagerMapper extends Mapper<NullWritable, MatImageWritable, Text, MatImageWritable>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] myCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            System.load(myCacheFiles[0].toUri().getPath());
        }

        @Override
        protected void map(NullWritable key, MatImageWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(value.getFileName()), value);
        }
    }

    static class SequenceFileMatImagePackagerReducer extends Reducer<Text, MatImageWritable, Text, MatImageWritable> {
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] myCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            System.load(myCacheFiles[0].toUri().getPath());
        }
    }
}
