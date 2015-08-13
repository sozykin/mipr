package experiments;


import openimaj.MBFImageInputFormat;
import openimaj.MBFImageOutputFormat;
import openimaj.MBFImageWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.openimaj.image.MBFImage;

import java.io.IOException;

/**
 * Created by Epanchee on 26.04.15.
 */
public class Img2Gray_OpenIMAJ {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = args[0];
        String output = args[1];

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(Img2Gray_OpenIMAJ.class);
        job.setMapperClass(Img2GrayOimgMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(MBFImageInputFormat.class);
        job.setOutputFormatClass(MBFImageOutputFormat.class);
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MBFImageWritable.class);
        outputPath.getFileSystem(conf).delete(outputPath, true); // delete folder if exists

        job.waitForCompletion(true);
    }

    public static class Img2GrayOimgMapper extends Mapper<NullWritable, MBFImageWritable, NullWritable, MBFImageWritable> {

        @Override
        protected void map(NullWritable key, MBFImageWritable value, Context context) throws IOException, InterruptedException {

            MBFImage image = value.getImage();

            MBFImage result = MBFImage.createRGB(image.getBand(0));

            context.write(NullWritable.get(), new MBFImageWritable(result, value.getFileName(), value.getFormat()));
        }
    }
}
