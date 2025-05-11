import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class MapSideJoin {

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private HashMap<String, String> customerMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader("customer.tbl"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split("\\|");
                    customerMap.put(tokens[0], line);
                }
                reader.close();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] orderTokens = value.toString().split("\\|");
            String custKey = orderTokens[1];

            String customer = customerMap.get(custKey);
            if (customer != null) {
                context.write(new Text(custKey), new Text(customer + "|" + value.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MapSideJoin <customer.tbl local path> <orders input dir> <output dir>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Map-Side Join");
        job.setJarByClass(MapSideJoin.class);
        job.setMapperClass(JoinMapper.class);
        job.setNumReduceTasks(0); // Map-only job

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Distributed cache
        job.addCacheFile(new URI(args[0] + "#customer.tbl"));

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
