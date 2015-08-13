package openimaj;

import core.recordReaders.ImageRecordWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.openimaj.image.ImageUtilities;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by Epanchee on 26.04.15.
 */
public class MBFImageRecordWriter extends ImageRecordWriter<MBFImageWritable> {
    public MBFImageRecordWriter(TaskAttemptContext taskAttemptContext){
        super(taskAttemptContext);
    }

    @Override
    protected void writeImage(MBFImageWritable image, FSDataOutputStream imageFile)
            throws IOException {
        ImageUtilities.write(image.getImage(), image.getFormat(), (OutputStream) imageFile);
    }
}
