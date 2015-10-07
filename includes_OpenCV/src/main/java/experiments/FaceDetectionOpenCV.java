package experiments;

import core.MiprConfigurationParser;
import opencv.MatImageInputFormat;
import opencv.MatImageOutputFormat;
import opencv.MatImageWritable;
import opencv.OpenCVMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencv.core.*;
import org.opencv.objdetect.CascadeClassifier;

import java.io.IOException;

/**
 * Created by Epanchee on 09.04.15.
 */
public class FaceDetectionOpenCV {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = args[0];
        String output = args[1];

        Job job = new MiprConfigurationParser().getOpenCVJobTemplate();
        job.setJarByClass(FaceDetectionOpenCV.class);
        job.setMapperClass(FaceDetectorMapper.class);
        job.setInputFormatClass(MatImageInputFormat.class);
        job.setOutputFormatClass(MatImageOutputFormat.class);
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MatImageWritable.class);

        job.waitForCompletion(true);
    }

    public static class FaceDetectorMapper extends OpenCVMapper<NullWritable, MatImageWritable, NullWritable, MatImageWritable> {

        @Override
        protected void map(NullWritable key, MatImageWritable value, Context context) throws IOException, InterruptedException {
            Mat image = value.getImage();

            if (image != null) {
                CascadeClassifier faceDetector = new CascadeClassifier("lbpcascade_frontalface.xml");
                MatOfRect faceDetections = new MatOfRect();
                faceDetector.detectMultiScale(image, faceDetections);

                for (Rect rect : faceDetections.toArray()) {
                    Core.rectangle(image, new Point(rect.x, rect.y), new Point(rect.x + rect.width, rect.y + rect.height), new Scalar(0, 0, 255), 3);
                }

                MatImageWritable matiw = new MatImageWritable(image);

                matiw.setFormat("jpg");
                matiw.setFileName(value.getFileName() + "_result");
                context.write(NullWritable.get(), matiw);
            }
        }
    }
}
