package mipr.job;

import mipr.image.BufferedImageWritable;
import mipr.imageapi.mapreduce.BufferedImageMapper;
import mipr.mapreduce.BufferedImageCombineInputFormat;
import mipr.mapreduce.BufferedImageInputFormat;
import mipr.mapreduce.BufferedImageOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Run MIPr Map-only Job using BufferedImage as image representation.
 * Every Map receive one image and process it using the ImageProcessor class, specified at command line.
 * Job uses FileInputFormat. Not suitable for small files.
 *
 * @see mipr.mapreduce.BufferedImageCombineInputFormat
 * @see mipr.imageapi.ImageProcessor
 *
 */

public class BufferedImageJob extends Configured implements Tool {

    protected Job basicSetup(String args[]) throws IOException {
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <image_processor_class> <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(-1);
        }

        Configuration conf = super.getConf();
        conf.set("mipr.biprocessor.class", args[0]);
        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setOutputFormatClass(BufferedImageOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(BufferedImageMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BufferedImageWritable.class);
        return job;
    }


    public int run(String[] args) throws Exception {

        Job job = basicSetup(args);

        job.setJobName("MIPr BufferedImage job");
        job.setInputFormatClass(BufferedImageInputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new BufferedImageJob(), args);
        System.exit(exitCode);
    }

}
