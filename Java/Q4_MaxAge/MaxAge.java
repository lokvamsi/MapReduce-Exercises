import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class maxAge {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split("\\t");
            if (input.length == 2) {
                String userid = input[0];
                String userfriends = input[1];
                context.write(new Text(userid), new Text(userfriends));
            }
        }
    }

    public static class MyReducer1 extends Reducer<Text, Text, Text, LongWritable> {
        private Map<String, Integer> user_age = new HashMap<>();

        private int getAge(String birthdate) {

            SimpleDateFormat df = new SimpleDateFormat("MM/DD/YYYY");
            Calendar birth = Calendar.getInstance();
            Date birthday;
            try {
                birthday = df.parse(birthdate);
                birth.setTime(birthday);
            } catch (ParseException e) {
                e.getMessage();
            }

            Calendar curDate = Calendar.getInstance();
            int age = curDate.get(Calendar.YEAR)
                    - birth.get(Calendar.YEAR);

            if (curDate.get(Calendar.MONTH) < birth.get(Calendar.MONTH)) {
                age--;
            } else {
                if (curDate.get(Calendar.MONTH) == birth.get(Calendar.MONTH) && curDate.get(Calendar.DAY_OF_MONTH) < birth.get(Calendar.DAY_OF_MONTH)) {
                    age--;
                }
            }
            return age;
        }

        protected void setup(Context context) throws IOException {
            Configuration config = context.getConfiguration();
            String user_data_path = config.get("user_data");

            Path user_path = new Path(user_data_path);
            FileSystem fs = FileSystem.get(config);
            FileStatus[] fss = fs.listStatus(user_path);
            for (FileStatus f : fss) {
                Path path = f.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

                String line = "";
                while ((line = br.readLine()) != null) {
                    String[] user_data = line.split(",");
                    int age = getAge(user_data[9]);
                    user_age.put(user_data[0], age);
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] friends = values.iterator().next().toString().split(",");
            int max_age = Integer.MIN_VALUE;
            for (String friend : friends) {
                max_age = Math.max(max_age, user_age.get(friend));
            }
            context.write(key, new LongWritable(max_age));
        }
    }

    public static class MyMapper2 extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] user_data = value.toString().split("\\t");
            if (user_data.length == 2) {
                String myId = user_data[0];
                String max_age = user_data[1];
                context.write(new LongWritable(Long.parseLong(max_age)), new Text(myId));
            }
        }
    }

    public static class MyReducer2 extends Reducer<LongWritable, Text, Text, Text> {
        private Map<String, String> user_addr = new HashMap<>();

        private int toptenages = 1;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            String user_data_path = config.get("user_data");

            Path user_path = new Path(user_data_path);
            FileSystem fs = FileSystem.get(config);
            FileStatus[] fss = fs.listStatus(user_path);
            for (FileStatus fileStatus : fss) {
                Path path = fileStatus.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line = "";
                while ((line = br.readLine()) != null) {
                    String[] user_details = line.split(",");
                    user_addr.put(user_details[0], user_details[3] + "," + user_details[4] + "," + user_details[5]);
                }
            }
        }

        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (toptenages <= 10) {
                    toptenages++;
                    context.write(value, new Text(user_addr.get(value.toString()) + "," + key.toString()));
                }
            }
        }
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        String[] other_args = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if (other_args.length != 4) {
            System.err.println("Usage : OldestFriend <input> <userdata> <intermediate output> <output>");
            System.exit(0);
        }

        String input = other_args[0];
        String userdata = other_args[1];
        String intermediate_output = other_args[2];
        String output = other_args[3];

        configuration.set("user_data", userdata);
        Job job1 = new Job(configuration, "maxAge");
        job1.setJarByClass(maxAge.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setMapperClass(MyMapper.class);
        job1.setReducerClass(MyReducer1.class);
        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(intermediate_output));

        if (!job1.waitForCompletion(true)) {
            System.exit(0);
        }

        configuration.set("user_data", userdata);
        Job job2 = new Job(configuration, "max age of friends");
        job2.setJarByClass(maxAge.class);

        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);

        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        FileInputFormat.addInputPath(job2, new Path(intermediate_output));
        FileOutputFormat.setOutputPath(job2, new Path(output));

        if (!job2.waitForCompletion(true)) {
            System.exit(0);
        }
        System.exit(1);
    }
}
