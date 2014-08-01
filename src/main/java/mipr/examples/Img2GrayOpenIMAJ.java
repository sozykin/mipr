package mipr.examples;

import mipr.image.FImageWritable;
import mipr.image.MBFImageWritable;
import mipr.mapreduce.FImageOutputFormat;
import mipr.mapreduce.MBFImageInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openimaj.image.FImage;
import org.openimaj.image.MBFImage;
import java.io.IOException;


/**
 * MIPR Job converting color image to grayscale example.
 * OpenIMAJ is used for conversion
 */
public class Img2GrayOpenIMAJ extends Configured implements Tool {
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(super.getConf(), "MIPR job converting color images to grayscale");
        job.setJarByClass(getClass());
        job.setInputFormatClass(MBFImageInputFormat.class);
        job.setOutputFormatClass(FImageOutputFormat.class);
        job.setMapperClass(Img2GrayOpenIMAJMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FImageWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new Img2GrayOpenIMAJ(), args);
        System.exit(exitCode);
    }

    public static class Img2GrayOpenIMAJMapper extends Mapper<NullWritable, MBFImageWritable,
            NullWritable, FImageWritable> {

        private FImage gray_image;
        private MBFImage color_image;
        private FImageWritable fiw = new FImageWritable();

        public void map(NullWritable key, MBFImageWritable value, Context context)
                throws IOException, InterruptedException {
            color_image = value.getImage();

            if (color_image != null) {

                gray_image = color_image.flatten();
                fiw.setFormat(value.getFormat());
                fiw.setFileName(value.getFileName());
                fiw.setImage(gray_image);
                context.write(NullWritable.get(), fiw);
            }
        }
    }

}
