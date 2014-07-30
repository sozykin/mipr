package mipr.image;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * MIPr image representation based on BufferedImage.
 * BufferedImageWritable implements Hadoop Writable interface
 * and can be used as a value in MapReduce programs
 */

public class BufferedImageWritable extends ImageWritable {

    // Internal Image representation
    private BufferedImage bi = null;

    public BufferedImageWritable(){

    }

    public BufferedImageWritable(BufferedImage bi){
        this.bi = bi;
    }

    public BufferedImageWritable(BufferedImage bi, String filename, String format){
        this.bi = bi;
        setFileName(filename);
        setFormat(format);
    }


    public BufferedImage getBufferedImage() {
        return bi;
    }

    public void setBufferedImage(BufferedImage bi) {
        this.bi = bi;
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);

        // Write image
        // Convert image to byte array
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bi, this.getFormat(), baos);
        baos.flush();
        byte[] bytes = baos.toByteArray();
        baos.close();
        // Write byte array size
        out.writeInt(bytes.length);
        // Write image bytes
        out.write(bytes);
    }

    public void readFields(DataInput in) throws IOException{
        super.readFields(in);

        // Read image byte array size
        int bArraySize = in.readInt();
        // Read image byte array
        byte[] bArray = new byte[bArraySize];
        in.readFully(bArray);
        // Read image from byte array
        bi = ImageIO.read(new ByteArrayInputStream(bArray));
    }

}
