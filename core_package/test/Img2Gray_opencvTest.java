import main.java.core.writables.MatImageWritable;
import main.java.experiments.Img2Gray_opencv;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.opencv.highgui.Highgui;

/**
 * Created by Epanchee on 09.03.15.
 */

@RunWith(JUnit4.class)
public class Img2Gray_opencvTest {
    MapDriver<NullWritable, MatImageWritable, NullWritable, MatImageWritable> mapDriver;
    MapReduceDriver<NullWritable, MatImageWritable, NullWritable, MatImageWritable, NullWritable, MatImageWritable> mapReduceDriver;

    @Before
    public void setUp() {
        Img2Gray_opencv.Img2Gray_opencvMapper mapper = new Img2Gray_opencv.Img2Gray_opencvMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver();
        mapReduceDriver.setMapper(mapper);
        System.load("C:\\Program Files (x86)\\opencv\\build\\java\\x64\\opencv_java2410.dll");
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(NullWritable.get(), new MatImageWritable(Highgui.imread(getClass().getResource("/best.jpg").getPath().substring(1))));
        mapDriver.withOutput(NullWritable.get(), new MatImageWritable(Highgui.imread("E:\\GitRepos\\mipr-2.0\\src\\main\\resources\\_gray.jpg")));
        mapDriver.runTest();
    }
}
