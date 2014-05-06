package mipr.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Subclass of BufferedImageRecordReader able to read images from several files
 *
 * @see org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat
 */
public class BufferedImageCombineRecordReader extends BufferedImageRecordReader {



    public BufferedImageCombineRecordReader(CombineFileSplit split,
                                             TaskAttemptContext context, Integer index) throws IOException{
        Configuration job = context.getConfiguration();
        final Path file = split.getPath(index);
        fileName = file.getName();
        FileSystem fs = file.getFileSystem(job);
        fileStream = fs.open(file);
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
    }


}
