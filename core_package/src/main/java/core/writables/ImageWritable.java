package core.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Epanchee on 17.02.15.
 */
public abstract class ImageWritable<I> implements Writable {

    protected I im;
    protected String fileName;
    protected String format;

    public I getImage(){
        return im;
    }

    public String getFormat() {
        return format;
    }

    public void write(DataOutput out) throws IOException {
        // Write image type
        Text.writeString(out, format);
        // Write image file name
        Text.writeString(out, fileName);
    }

    public void readFields(DataInput in) throws IOException{
        // Read image type
        format = Text.readString(in);
        // Read image file name
        fileName = Text.readString(in);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fName) {
        fileName = fName;
    }

    public void setFormat(String fFormat) {
        format = fFormat;
    }

    public void setImage(I bi) {
        im = bi;
    }
}
