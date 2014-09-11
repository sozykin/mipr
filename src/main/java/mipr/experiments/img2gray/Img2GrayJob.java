package mipr.experiments.img2gray;

import mipr.image.BufferedImageWritable;
import mipr.job.HadoopJob;
import mipr.job.HadoopJobConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;

/**
 * Created by Timofey on 04.09.14.
 */
public class Img2GrayJob extends HadoopJob {
    public Img2GrayJob() {
        super(Img2GrayMapper.class, new HadoopJobConfiguration("<input> <output>\n", 2, "Hadoop Img2gray  job"));
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new Img2GrayJob(), args);
        System.exit(exitCode);
    }

    public static class Img2GrayMapper extends Mapper<NullWritable,BufferedImageWritable,
            NullWritable,BufferedImageWritable>{

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
