package mipr.job;

/**
 * Created by Timofey on 09.09.14.
 */
public class HadoopJobConfiguration {

    private String usage_string = "No usage string";
    private String job_info = "Null info";
    private int args_length;

    public HadoopJobConfiguration(int args_length){
        this.args_length = args_length;
    }

    public HadoopJobConfiguration(String usage_string, int args_length,  String job_info){
        this(args_length);
        this.job_info = job_info;
        this.usage_string = usage_string;
    }

    public String getUsageString(){
        return usage_string;
    }

    public String getJobInfo() {
        return job_info;
    }

    public int getArgsLength() {
        return args_length;
    }
}
