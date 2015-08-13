package opencv;

import core.recordReaders.ImageRecordReader;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;

import java.io.IOException;

/**
 * Created by Epanchee on 24.02.15.
 */
public class MatImageRecordReader extends ImageRecordReader<MatImageWritable> {
    @Override
    protected MatImageWritable readImage(FSDataInputStream fileStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[16384];

        while ((nRead = fileStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }

        buffer.flush();
        byte[] temporaryImageInMemory = buffer.toByteArray();
        buffer.close();

        Mat out = Highgui.imdecode(new MatOfByte(temporaryImageInMemory), Highgui.IMREAD_ANYCOLOR);

        return new MatImageWritable(out);
    }
}
