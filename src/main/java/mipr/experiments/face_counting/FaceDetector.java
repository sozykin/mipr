package mipr.experiments.face_counting;

import mipr.image.FImageWritable;
import mipr.image.ImageWritable;
import mipr.job.HadoopJob;
import mipr.job.HadoopJobConfiguration;
import mipr.mapreduce.FImageInputFormat;
import mipr.mapreduce.FImageOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.openimaj.image.FImage;
import org.openimaj.image.processing.face.detection.DetectedFace;
import org.openimaj.image.processing.face.detection.HaarCascadeDetector;

import java.io.IOException;
import java.util.List;

/**
 * Created by Timofey on 15.09.14.
 */
public class FaceDetector extends HadoopJob {
    public FaceDetector() {
        super(FaceDetectorMapper.class, new HadoopJobConfiguration("<input> <output>\n", 2, "MIPR face detector job\n"));
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FaceDetector myJob = new FaceDetector();

        myJob.setIFormat(FImageInputFormat.class);
        myJob.setOFormat(FImageOutputFormat.class);
        myJob.setOKey(NullWritable.class);
        myJob.setOValue(FImageWritable.class);

        int exitCode = ToolRunner.run(conf, myJob, args);
        System.exit(exitCode);
    }

    public static class FaceDetectorMapper extends Mapper <NullWritable, FImageWritable,
            NullWritable, FImageWritable>{

        private FImage image;
        private FImageWritable result_image = new FImageWritable();

        @Override
        protected void map(NullWritable key, FImageWritable value, Context context) throws IOException, InterruptedException {
            image = value.getImage();

            if (image != null) {
                org.openimaj.image.processing.face.detection.FaceDetector<DetectedFace,FImage> fd = new HaarCascadeDetector(40);
                List<DetectedFace> faces = fd.detectFaces(image);
                FImage faces_image = new FImage(image.width, image.height);
                for (DetectedFace face : faces) {
                    faces_image.addInplace(face.getFacePatch());
                }

                result_image.setImage(faces_image);
                result_image.setFormat(ImageWritable.JPEG_FORMAT);
                result_image.setFileName(value.getFileName());

                context.write(NullWritable.get(), result_image);
            }

        }
    }
}
