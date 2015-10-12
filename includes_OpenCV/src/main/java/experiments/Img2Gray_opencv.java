package experiments;

import core.MiprConfigurationParser;
import opencv.MatImageInputFormat;
import opencv.MatImageOutputFormat;
import opencv.MatImageWritable;
import opencv.OpenCVMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;

import java.io.IOException;

/**
 * Created by Epanchee on 24.02.15.
 */
public class Img2Gray_opencv extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res  = ToolRunner.run(new Img2Gray_opencv(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];

        MiprConfigurationParser confParser = new MiprConfigurationParser();
        Job job = confParser.getOpenCVJobTemplate();
        job.setJarByClass(Img2Gray_opencv.class);
        job.setMapperClass(Img2Gray_opencvMapper.class);
        job.setInputFormatClass(MatImageInputFormat.class);
        job.setOutputFormatClass(MatImageOutputFormat.class);
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, input);
        FileInputFormat.setMaxInputSplitSize(job, confParser.getMaxSplitSize());
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MatImageWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Img2Gray_opencvMapper extends OpenCVMapper<NullWritable, MatImageWritable, NullWritable, MatImageWritable>{
        protected void map(NullWritable key, MatImageWritable value, Context context) throws IOException, InterruptedException {
            Mat image = value.getImage();
            Mat result = new Mat(image.height(), image.width(), CvType.CV_8UC3);

            if (image.type() == CvType.CV_8UC3) {
                Imgproc.cvtColor(image, result, Imgproc.COLOR_RGB2GRAY);
            } else result = image;

            context.write(NullWritable.get(), new MatImageWritable(result, value.getFileName(), value.getFormat()));
        }
    }
}
