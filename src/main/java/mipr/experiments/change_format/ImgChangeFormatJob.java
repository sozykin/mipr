package mipr.experiments.change_format;

import mipr.image.BufferedImageWritable;
import mipr.job.HadoopJob;
import mipr.job.HadoopJobConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Timofey on 04.09.14.
 */
public class ImgChangeFormatJob extends HadoopJob{
    public ImgChangeFormatJob() {
        super(Img2ChangeFormatMapper.class, new HadoopJobConfiguration("<input> <output> <format>\n", 3, "Hadoop Img2PNG  job"));
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new ImgChangeFormatJob(), args);
        System.exit(exitCode);
    }

    @Override
    protected void preprocess(String[] args, Job job) {
        Configuration conf = job.getConfiguration();
        conf.set("format", args[2]);
    }

    public static class Img2ChangeFormatMapper extends Mapper<NullWritable,BufferedImageWritable,
            NullWritable,BufferedImageWritable>{

        public void map(NullWritable key, BufferedImageWritable value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            value.setFormat(conf.get("format"));
            context.write(NullWritable.get(), value);
        }

    }

}
