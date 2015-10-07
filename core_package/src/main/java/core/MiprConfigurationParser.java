package core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

public class MiprConfigurationParser {

    public MiprConfigurationParser() {
        conf = new Configuration();

        JSONParser jparser = new JSONParser();
        JSONObject settings = null;
        try {
            settings = (JSONObject) jparser.parse(new FileReader(getClass().getClassLoader().getResource("main.json").getFile()));
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        try {
            JSONObject miprconf = (JSONObject) settings.get("mipr-conf");
            opencvpath = (String) miprconf.get("opencvpath");
            maxsplitsize = (long) miprconf.get("maxsplitsize");

            JSONObject javaconf = (JSONObject) settings.get("java-conf");
            for (Object o : javaconf.keySet()) {
                String key = (String) o;
                conf.set(key, (String) javaconf.get(o));
            }
        } catch (NullPointerException e) {
            System.out.println("Error occured while parsing configuration file. Check your core_package/src/configuration/main.json file.");
        }
    }

    private String opencvpath = "";
    private long maxsplitsize = 134217728;
    private Configuration conf;

    public URI getOpenCVUri(){
        return new Path(opencvpath).toUri();
    }

    public long getMaxSplitSize(){
        return maxsplitsize;
    }

    public Job getOpenCVJobTemplate() throws IOException {
        return getOpenCVJobTemplate(conf);
    }

    public Job getOpenCVJobTemplate(Configuration conf) throws IOException {
        DistributedCache.addCacheFile(getOpenCVUri(), conf);
        Job job = new Job(conf);
        job.setNumReduceTasks(0);

        return job;
    }
}