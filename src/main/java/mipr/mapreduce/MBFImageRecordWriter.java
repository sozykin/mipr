package mipr.mapreduce;

import mipr.image.MBFImageWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.openimaj.image.ImageUtilities;

import java.io.IOException;
import java.io.OutputStream;

/**
 * RecordWriter for MBFImage (color image format from OpenIMAJ library)
 */
public class MBFImageRecordWriter extends ImageRecordWriter<MBFImageWritable> {


    MBFImageRecordWriter(TaskAttemptContext taskAttemptContext){
        super(taskAttemptContext);
    }

    @Override
    protected void writeImage(MBFImageWritable image, FSDataOutputStream imageFile)
            throws IOException {
        ImageUtilities.write(image.getImage(),
                image.getFormat(), (OutputStream) imageFile);
    }
}
