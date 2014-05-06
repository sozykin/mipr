package mipr.job;

import mipr.mapreduce.BufferedImageCombineInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Run MIPr Map-only Job using BufferedImage as image representation.
 * Every Map receive one image and process it using the ImageProcessor class, specified at command line.
 * Job uses CombineInputFormat. Suitable for small files.
 *
 * @see mipr.mapreduce.BufferedImageCombineInputFormat
 * @see mipr.imageapi.ImageProcessor
 *
 */

public class CombineBufferedImageJob extends BufferedImageJob implements Tool {

    public int run(String[] args) throws Exception {

        Job job = basicSetup(args);

        job.setJobName("MIPr CombineBufferedImage job");
        job.setInputFormatClass(BufferedImageCombineInputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new CombineBufferedImageJob(), args);
        System.exit(exitCode);
    }





}
