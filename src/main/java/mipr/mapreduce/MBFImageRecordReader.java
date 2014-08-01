package mipr.mapreduce;

import mipr.image.MBFImageWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.openimaj.image.ImageUtilities;
import org.openimaj.image.MBFImage;


/**
 * RecordReader for Color OpenIMAJ image (MBFImage)
 */
public class MBFImageRecordReader extends ImageRecordReader<MBFImageWritable> {

    protected MBFImageWritable createImageWritable(){
        return new MBFImageWritable();
    }

    protected MBFImageWritable readImage(FSDataInputStream fsDataInputStream){
        MBFImageWritable mbfiw = new MBFImageWritable();
        MBFImage mbfi;
        try {
            mbfi = ImageUtilities.readMBF(fsDataInputStream);
        } catch (Exception e) {
            mbfi = null;
        }
        mbfiw.setImage(mbfi);
        return mbfiw;
    }

}
