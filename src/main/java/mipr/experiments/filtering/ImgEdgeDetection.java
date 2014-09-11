package mipr.experiments.filtering;

import mipr.image.BufferedImageWritable;
import mipr.job.HadoopJob;
import mipr.job.HadoopJobConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONObject;

import java.awt.image.BufferedImage;
import java.awt.image.BufferedImageOp;
import java.awt.image.ConvolveOp;
import java.awt.image.Kernel;
import java.io.File;
import java.io.IOException;

/**
 * Created by Timofey on 04.09.14.
 */
public class ImgEdgeDetection extends HadoopJob {
    public ImgEdgeDetection() {
        super(ImgEdgeDetectionMapper.class, new HadoopJobConfiguration("<input> <output>\n", 2, "Hadoop ImgEdgeDetection  job"));
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new ImgEdgeDetection(), args);
        System.exit(exitCode);
    }

    public static class ImgEdgeDetectionMapper extends Mapper<NullWritable,BufferedImageWritable,
            NullWritable,BufferedImageWritable>{

        private int dimension;
        private float[] kernel;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            File file = new File("kernel.json");
            String str = FileUtils.readFileToString(file, "utf-8");
            JSONObject json = new JSONObject(str);
            JSONArray jsonker = json.getJSONArray("kernel");

            float [] ker = new float[jsonker.length()];
            for(int i = 0; i<jsonker.length(); i++){
                ker[i] = Float.parseFloat(jsonker.get(i).toString());
            }

            dimension = (int) json.get("dim");
            kernel = ker;

        }

        public void map(NullWritable key, BufferedImageWritable value, Context context)
                throws IOException, InterruptedException {
            BufferedImage sourceImg = value.getImage();

            if (sourceImg != null) {

/*                float[] kernel = {
                        0.0f, -1.0f, 0.0f,
                        -1.0f, 4.0f, -1.0f,
                        0.0f, -1.0f, 0.0f
                };*/
                BufferedImageOp kernelFilter = new ConvolveOp(new Kernel(dimension, dimension, kernel));
                BufferedImage destinationImg = kernelFilter.filter(sourceImg, null);
                BufferedImageWritable biw = new BufferedImageWritable(destinationImg, value.getFileName(), value.getFormat());
                context.write(NullWritable.get(), biw);
            }

        }

    }
}
