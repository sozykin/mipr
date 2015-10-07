package experiments;

import core.MiprConfigurationParser;
import opencv.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgproc.Imgproc;

import java.io.IOException;

public class CannyEdgeDetector {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = args[0];
        String output = args[1];

        long startTime = System.currentTimeMillis();

        Job job = new MiprConfigurationParser().getOpenCVJobTemplate();
        job.setJarByClass(CannyEdgeDetector.class);
        job.setMapperClass(CannyMapper.class);
        job.setInputFormatClass(CombineMatImageInputFormat.class);
//        job.setInputFormatClass(MatImageInputFormat.class);
        job.setOutputFormatClass(MatImageOutputFormat.class);
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MatImageWritable.class);

        job.waitForCompletion(true);

        long estimatedTime = System.currentTimeMillis() - startTime;

        System.out.println("Job has taken " + estimatedTime + " ms");
    }

    public static class CannyMapper extends OpenCVMapper<NullWritable, MatImageWritable, NullWritable, MatImageWritable> {
        @Override
        protected void map(NullWritable key, MatImageWritable value, Context context) throws IOException, InterruptedException {
            Mat image = value.getImage();
            Mat rimamge = new Mat();

            Imgproc.blur(image, image, new Size(5,5));
            Imgproc.Canny(image, rimamge, 1, 50);

            context.write(NullWritable.get(), new MatImageWritable(rimamge, value.getFileName(), value.getFormat()));
        }
    }
}