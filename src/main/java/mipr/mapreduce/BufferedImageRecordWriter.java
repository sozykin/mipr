package mipr.mapreduce;

import mipr.image.BufferedImageWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import javax.imageio.ImageIO;
import java.io.IOException;

/**
 * RecordWriter for BufferedImage
 */

public class BufferedImageRecordWriter extends ImageRecordWriter<BufferedImageWritable> {


    BufferedImageRecordWriter(TaskAttemptContext taskAttemptContext){
        super(taskAttemptContext);
    }

    @Override
    protected void writeImage(BufferedImageWritable image, FSDataOutputStream imageFile) throws IOException {
        ImageIO.write(image.getImage(), image.getFormat(), imageFile);
    }
}
