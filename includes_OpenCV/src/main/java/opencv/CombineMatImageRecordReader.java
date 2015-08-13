package opencv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Created by Epanchee on 26.05.2015.
 */
public class CombineMatImageRecordReader extends MatImageRecordReader {
    public CombineMatImageRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
        Configuration job = context.getConfiguration();
        final Path file = split.getPath(index);
        fileName = file.getName();
        FileSystem fs = file.getFileSystem(job);
        fileStream = fs.open(file);
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    }
}
