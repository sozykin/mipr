package mipr.mapreduce;

import mipr.image.FImageWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.openimaj.image.ImageUtilities;

import java.io.IOException;
import java.io.OutputStream;

/**
 * RecordWriter for BufferedImage
 */

public class FImageRecordWriter extends ImageRecordWriter<FImageWritable> {


    FImageRecordWriter(TaskAttemptContext taskAttemptContext){
        super(taskAttemptContext);
    }

    @Override
    protected void writeImage(FImageWritable image, FSDataOutputStream imageFile) throws IOException {
        ImageUtilities.write(image.getImage(),
                image.getFormat(), (OutputStream) imageFile);
    }
}
