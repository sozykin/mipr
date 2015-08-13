package experiments;

import core.formats.BufferedImage.BufferedImageInputFormat;
import core.formats.BufferedImage.BufferedImageOutputFormat;
import core.writables.BufferedImageWritable;
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
 * Created by Epanchee on 13.07.2015.
 */
public class Img2Gray extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res  = ToolRunner.run(new Img2Gray(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job  = Job.getInstance(getConf(), "Img2Gray job"); // name of your job. It needs for logs.
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job, String.valueOf(new Path(args[0]))); // input folder
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output folder. Must not exists before the start!
        job.setNumReduceTasks(0); // In our case we don't need Reduce phase
        job.setInputFormatClass(BufferedImageInputFormat.class);
        job.setOutputFormatClass(BufferedImageOutputFormat.class);
        job.setMapperClass(Img2GrayMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BufferedImageWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Img2GrayMapper extends Mapper<NullWritable, BufferedImageWritable, NullWritable, BufferedImageWritable> {

        private Graphics g;
        private BufferedImage grayImage;

        @Override
        protected void map(NullWritable key, BufferedImageWritable value, Context context) throws IOException, InterruptedException {
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
