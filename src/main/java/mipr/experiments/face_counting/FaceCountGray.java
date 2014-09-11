package mipr.experiments.face_counting;

import mipr.image.FImageWritable;
import mipr.job.HadoopJob;
import mipr.job.HadoopJobConfiguration;
import mipr.mapreduce.FImageInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.openimaj.image.FImage;
import org.openimaj.image.processing.face.detection.DetectedFace;
import org.openimaj.image.processing.face.detection.FaceDetector;
import org.openimaj.image.processing.face.detection.HaarCascadeDetector;

import java.io.IOException;
import java.util.List;

/**
 * Created by Timofey on 09.09.14.
 */
public class FaceCountGray extends HadoopJob {
    public FaceCountGray() {
        super(FaceCounterGrayMapper.class, new HadoopJobConfiguration("<input> <output>\n", 2, "MIPR face counting job\n"));
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FaceCountGray myJob = new FaceCountGray();

        myJob.setIFormat(FImageInputFormat.class);
        myJob.setOFormat(TextOutputFormat.class);
        myJob.setOKey(Text.class);
        myJob.setOValue(IntWritable.class);

        int exitCode = ToolRunner.run(conf, myJob, args);
        System.exit(exitCode);
    }

    public static class FaceCounterGrayMapper extends Mapper <NullWritable, FImageWritable,
            Text, IntWritable>{

        private FImage image;
        private Text fileName = new Text();
        private IntWritable faceCount = new IntWritable();

        @Override
        protected void map(NullWritable key, FImageWritable value, Context context) throws IOException, InterruptedException {
            image = value.getImage();

            if (image != null) {
                FaceDetector<DetectedFace,FImage> fd = new HaarCascadeDetector(40);
                List<DetectedFace> faces = fd.detectFaces(image);
                faceCount.set(faces.size());
                fileName.set(value.getFileName() + "." + value.getFormat());
                context.write(fileName, faceCount);
            }

        }
    }
}
