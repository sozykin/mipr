package mipr.image;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Base class for image representation in MIPr.
 * ImageWritable implements Hadoop Writable interface
 * and can be used as a value in MapReduce programs
 */

public abstract class ImageWritable<I> implements Writable {

    public static String JPEG_FORMAT = "jpg";

    /**
     * Format of image, e.g. jpg, png, etc.
     */
    private String format = JPEG_FORMAT;

    /**
     * Image filename without extension
     */
    private String fileName;

    /**
     * Image representation
     */
    protected I im;

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
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

    public I getImage() {
        return im;
    }

    public void setImage(I im) {
        this.im = im;
    }


}