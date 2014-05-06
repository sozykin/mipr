package mipr.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Base class for MIPr image input formats
 */
public abstract class ImageInputFormat<K,V> extends FileInputFormat {

    /**
     *  Return false because image cannot be splitted
     */
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

}
