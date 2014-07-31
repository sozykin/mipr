package mipr.mapreduce;

import mipr.image.FImageWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.openimaj.image.FImage;
import org.openimaj.image.ImageUtilities;

import java.io.InputStream;

/**
 * RecordReader for GrayScale OpenIMAJ image (FImage)
 */

public class FImageRecordReader extends ImageRecordReader<FImageWritable> {

    protected FImageWritable createImageWritable(){
        return new FImageWritable();
    }

    protected FImageWritable readImage(FSDataInputStream fsDataInputStream){
        FImageWritable fiw = new FImageWritable();
        FImage fi;
        try {
            fi = ImageUtilities.readF((InputStream) fsDataInputStream);
        } catch (Exception e) {
            fi = null;
        }
        fiw.setImage(fi);
        return fiw;
    }

}
