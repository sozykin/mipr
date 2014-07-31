package mipr.experiments.sequence;

import mipr.image.BufferedImageWritable;
import mipr.mapreduce.BufferedImageCombineInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

/**
 * Created by u1213 on 5/2/14.
 */
public class SequenceOutputJob extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(super.getConf(), "Hadoop SequenceOutputJob job");
        job.setJarByClass(getClass());
        job.setInputFormatClass(BufferedImageCombineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapperClass(MyMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BufferedImageWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new SequenceOutputJob(), args);
        System.exit(exitCode);
    }

    public static class MyMapper extends Mapper<NullWritable,BufferedImageWritable,
            Text,BufferedImageWritable> {


        public void map(NullWritable key, BufferedImageWritable value, Context context)
                throws IOException, InterruptedException {

            if (value.getImage() != null) {
                context.write(new Text(value.getFileName()), value);
            }

        }
    }
}
