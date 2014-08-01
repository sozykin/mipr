package mipr.image;

import mipr.utils.OpenIMAJUtilities;
import org.openimaj.image.MBFImage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * MIPr image representation based on MBFImage -
 * a color image from OpenIMAJ.
 * MBFImageWritable implements Hadoop Writable interface
 * and can be used as a value in MapReduce programs
 */
public class MBFImageWritable extends ImageWritable<MBFImage> {

    public MBFImage getImage() {
        return im;
    }

    public void setImage(MBFImage mbfi) {
        this.im = mbfi;
    }


    public void write(DataOutput out) throws IOException {
        super.write(out);

        // Determining size of image
        int width = im.getWidth();
        int height = im.getHeight();

        // Writing size of image
        out.writeInt(width);
        out.writeInt(height);

        // Write number of band
        out.writeInt(im.bands.size());

        // Write pixels
        out.write(OpenIMAJUtilities.mbfImagePixelsToByteArray(im));
    }
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        // Reading image size
        int width = in.readInt();
        int height = in.readInt();

        // Read number of band
        int n = in.readInt();

        // Creating buffer for pixels
        byte[] bPixels = new byte[4 * width * height * n];

        // Reading pixel data
        in.readFully(bPixels);

        im = new MBFImage(width, height, n);

        OpenIMAJUtilities.setMBFImagePixelsFromByteArray(im, bPixels);
    }
}
