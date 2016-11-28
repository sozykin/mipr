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
import org.openimaj.image.FImage;
import org.openimaj.image.MBFImage;
import org.openimaj.image.colour.RGBColour;
import org.openimaj.image.colour.Transforms;
import org.openimaj.image.processing.face.detection.DetectedFace;
import org.openimaj.image.processing.face.detection.FaceDetector;
import org.openimaj.image.processing.face.detection.HaarCascadeDetector;
import org.openimaj.image.processing.face.detection.keypoints.FKEFaceDetector;
import org.openimaj.image.processing.face.detection.keypoints.KEDetectedFace;
import org.openimaj.image.processing.face.util.SimpleDetectedFaceRenderer;

import java.io.IOException;
import java.util.List;

/**
 * Created by smartkit on 2016/11/27.
 */
public class Img2FaceDetect_OpenIMAJ {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = args[0];
        String output = args[1];

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(Img2FaceDetect_OpenIMAJ.class);
        job.setMapperClass(Img2FaceDetect_OpenIMAJ.Img2FaceDetectOimgMapper.class);
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

    public static class Img2FaceDetectOimgMapper extends Mapper<NullWritable, MBFImageWritable, NullWritable, MBFImageWritable> {

        @Override
        protected void map(NullWritable key, MBFImageWritable value, Context context) throws IOException, InterruptedException {

            MBFImage image = value.getImage();

//            MBFImage result = MBFImage.createRGB(image.getBand(0));
            // A simple Haar-Cascade face detector,see more: http://www.programcreek.com/java-api-examples/index.php?api=org.openimaj.image.ImageUtilities
            HaarCascadeDetector fd = new HaarCascadeDetector();
//            FaceDetector<KEDetectedFace,FImage> fd = new FKEFaceDetector();
            List<DetectedFace> faces = fd.detectFaces(Transforms.calculateIntensity( image ));
            System.out.print("DetectedFaces:"+faces.size()+"\n");
            for( DetectedFace face : faces) {
                image.drawShape(face.getBounds(), RGBColour.GREEN);
            }

            context.write(NullWritable.get(), new MBFImageWritable(image, value.getFileName(), value.getFormat()));
        }
    }
}
