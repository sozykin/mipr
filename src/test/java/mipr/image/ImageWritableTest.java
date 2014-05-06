package mipr.image;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;

/**
 * Created by u1213 on 4/5/14.
 */

@RunWith(JUnit4.class)
public class ImageWritableTest {

    private final String TEST_FILE_NAME = "test.jpg";
    private final String TEST_FILE_FORMAT = "jpg";

    @Test
    public void testSerialization() throws IOException {

        ImageWritable im = new ImageWritable();
        im.setFileName(TEST_FILE_NAME);
        im.setFormat(TEST_FILE_FORMAT);

        // Write image to the stream
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        im.write(new DataOutputStream(out));

        // Read image from byte array
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        im.readFields(new DataInputStream(in));

        org.junit.Assert.assertEquals("Problems with image file name serialization", im.getFileName(), TEST_FILE_NAME);

        org.junit.Assert.assertEquals("Problems with image format serialization", im.getFormat(), TEST_FILE_FORMAT);


    }
}
