package opencv;

import core.recordReaders.ImageRecordWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;

/**
 * Created by Epanchee on 24.02.15.
 */
public class MatImageRecordWriter extends ImageRecordWriter<MatImageWritable> {
    public MatImageRecordWriter(TaskAttemptContext taskAttemptContext) {
        super(taskAttemptContext);
    }

    @Override
    protected void writeImage(MatImageWritable image, FSDataOutputStream imageFile) throws Exception {
        MatOfByte mob = new MatOfByte();
        Highgui.imencode("." + image.getFormat(), image.getImage(), mob);
        imageFile.write(mob.toArray());
    }
}
