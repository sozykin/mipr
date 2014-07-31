package mipr.experiments.img2gray;

import mipr.image.BufferedImageWritable;
import mipr.mapreduce.BufferedImageCombineInputFormat;
import mipr.mapreduce.BufferedImageOutputFormat;
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

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;

/**
 * Created by u1213 on 5/2/14.
 */
public class Img2GrayJob extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(super.getConf(), "Hadoop Img2Gray  job");
        job.setJarByClass(getClass());
        job.setInputFormatClass(BufferedImageCombineInputFormat.class);
        job.setOutputFormatClass(BufferedImageOutputFormat.class);
        job.setMapperClass(Im2GrayMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BufferedImageWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new Img2GrayJob(), args);
        System.exit(exitCode);
    }

    public static class Im2GrayMapper extends Mapper<NullWritable,BufferedImageWritable,
            NullWritable,BufferedImageWritable> {

        private Graphics g;
        private BufferedImage grayImage;

        public void map(NullWritable key, BufferedImageWritable value, Context context)
                throws IOException, InterruptedException {
            BufferedImage colorImage = value.getImage();

            if (colorImage != null) {

                grayImage = new BufferedImage(colorImage.getWidth(), colorImage.getHeight(),
                        BufferedImage.TYPE_BYTE_GRAY);
                g = grayImage.getGraphics();
                g.drawImage(colorImage, 0, 0, null);
                g.dispose();
                BufferedImageWritable biw = new BufferedImageWritable(grayImage, value.getFileName(), value.getFormat());
                context.write(NullWritable.get(), biw);
            }

        }
    }
}
