package mipr.mapreduce;

import mipr.image.BufferedImageWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * InputFormat for BufferedImageWritable
 * Read several images at ones to improve speed.
 * Suitable for small images
 *
 * @see org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat
 * @see mipr.mapreduce.ImageInputFormat
 * @see mipr.image.BufferedImageWritable
 */


public class BufferedImageCombineInputFormat extends CombineFileInputFormat<NullWritable, BufferedImageWritable> {

    public BufferedImageCombineInputFormat(){
        super();
        setMaxSplitSize(67108864);
    }

    public RecordReader<NullWritable,BufferedImageWritable> createRecordReader(InputSplit split,
                       TaskAttemptContext context) throws IOException {
        return new
                CombineFileRecordReader<NullWritable,BufferedImageWritable>((CombineFileSplit)split, context,
                BufferedImageCombineRecordReader.class);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file){
        return false;
    }
}
