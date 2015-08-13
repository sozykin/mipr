# MIPr - MapReduce Image Processing framework for Hadoop

MIPr provides the ability to process images in Hadoop.

MIPr includes:

* Writable Wrappers for images
* InputFormat and OutputFormat for images
* Several Jobs for image processing
* OpenCV and OpenIMAJ support

## Installation

### Prerequisites

* Java 7 (preferably Oracle)
* Maven 3.2.5

### Building

1. Clone repository with MIPr sources

    `git clone https://github.com/sozykin/mipr.git`

2. Build package by using Apache Maven

    To build full package with OpenIMAJ and OpenCV support run

    `mvn package`

    Notice that size of the package will be greater than separate build

    To build separate packages run

    `mvn package -pl [desired_package] -am`

    Where **desired_package** is one of the followings:

    - core_package
    - includes_OpenCV (includes core with OpenCV support)
    - includes_OpenIMAJ (includes core with OpenIMAJ support)

3. It will build jar file *...-jar-with-dependencies.jar* and place it in the *target* folder.

### Running

1. Copy image files to HDFS:

    `$ hadoop fs -copyFromLocal local_image_folder hdfs_image_folder`

2. Run test MIPr Job which converts color images to grayscale:

    `$ hadoop jar mipr-core-0.1-jar-with-dependencies.jar experiments.Img2Gray hdfs_image_folder hdfs_output_folder`

3. Copy processed images back from HDFS to the local filesystem:

    `$ hadoop fs -copyToLocal hdfs_output_folder local_output_folder`

4. Check that images were converted correctly.

### Creating your own Hadoop job

To process images by your own way you need to create one class. For example, lets create job, which processes color images to grayscale by using OpenCV.
For now, MIPr already has this class which placed in *includes_OpenCV\src\main\java\experiments\Img2Gray_opencv*.

1. Create public class inherited from **Configured** superclass and **Tool** interface.

    ```java
    public class Img2Gray_opencv extends Configured implements Tool{
        public static void main(String[] args) throws Exception {
            int res  = ToolRunner.run(new Img2Gray(), args);
            System.exit(res);
        }
    ```

2. Create **run** method inside your class. Fill it regarding library you will use.

    ```java
    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];

        Job job = MiprMain.getOpenCVJobTemplate();
        job.setJarByClass(Img2Gray_opencv.class);
        job.setMapperClass(Img2Gray_opencvMapper.class);
        job.setInputFormatClass(MatImageInputFormat.class);
        job.setOutputFormatClass(MatImageOutputFormat.class);
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MatImageWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
    ```

    Most important configurations are:

    - job.setInputFormatClass([InputFormat].class)

      Where [InputFormat] one of the following:

      * Java 2D

        **BufferedImageInputFormat**
      * OpenIMAJ

        **MBFImageInputFormat**
      * OpenCV

        **MatImageInputFormat**

        **CombineMatImageInputFormat**
    - job.setOutputFormatClass([OutputFormat].class)

       Where [OutputFormat] is similar to [InputFormat]

    - job.setMapperClass([MapperClass].class)

        Where [MapperClass] is your implemented Mapper class which contains map-method.

    - job.setOutputKeyClass(NullWritable.class)

        In most cases of image processing Key class doesn't necessary. You can leave it by using special NullWritable hadoop-class which contains nothing.

    - job.setOutputValueClass([Value].class)

        [Value] depends on which library you are going to use.

      * Java 2D

        **BufferedImageWritable**
      * OpenIMAJ

        **MBFImageWritable**
      * OpenCV

        **MatImageWritable**

3. Create Mapper class. Your class should extend OpenCVMapper superclass to make available usage of OpenCV library in parallel mode. Method **map** contains image processing algorithm.

    ```java
        public static class Img2Gray_opencvMapper extends OpenCVMapper<NullWritable, MatImageWritable, NullWritable, MatImageWritable>{
            protected void map(NullWritable key, MatImageWritable value, Context context) throws IOException, InterruptedException {
                Mat image = value.getImage();
                Mat result = new Mat(image.height(), image.width(), CvType.CV_8UC3);

                if (image.type() == CvType.CV_8UC3) {
                    Imgproc.cvtColor(image, result, Imgproc.COLOR_RGB2GRAY);
                } else result = image;

                context.write(NullWritable.get(), new MatImageWritable(result, value.getFileName(), value.getFormat()));
            }
        }
    ```

4. Return to **running section** and build package including your own hadoop-job.