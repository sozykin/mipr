package mipr.examples;

import mipr.image.FImageWritable;
import mipr.mapreduce.FImageInputFormat;
import mipr.mapreduce.FImageOutputFormat;
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
import org.openimaj.image.processing.face.detection.DetectedFace;
import org.openimaj.image.processing.face.detection.FaceDetector;
import org.openimaj.image.processing.face.detection.HaarCascadeDetector;

import java.io.IOException;
import java.util.List;

/**
 * MIPR Job face counting example. OpenIMAJ is used for face recognition
 */
public class FaceDetection extends Configured implements Tool {
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(super.getConf(), "MIPR face counting job");
        job.setJarByClass(getClass());
        job.setInputFormatClass(FImageInputFormat.class);
        job.setOutputFormatClass(FImageOutputFormat.class);
        job.setMapperClass(FaceDetectionMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FImageWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new FaceDetection(), args);
        System.exit(exitCode);
    }

    public static class FaceDetectionMapper extends Mapper<NullWritable, FImageWritable,
            NullWritable, FImageWritable> {

        private FImage image;

        public void map(NullWritable key, FImageWritable value, Context context)
                throws IOException, InterruptedException {
            image = value.getImage();

            if (image != null) {
                FaceDetector<DetectedFace,FImage> fd = new HaarCascadeDetector(40);
                List<DetectedFace> faces = fd.detectFaces(image);
                for( DetectedFace face : faces ) {
                    image.drawShape(face.getShape(), 3, 1f);
                }
                context.write(NullWritable.get(), value);
            }

        }
    }

}
