import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.*;
import java.io.IOException;

public class Q2_Top_10
{

    // Record Reader reads input from input file line by line and converts it to key,value pairs for mapper
    // Input file type : TextInputFormat file. Therefore o/p of RecordReader is of format (byteoffset, <entire line>)
    // Method will form pairs of friends such that smaller id is on the left side
    // INPUT:  (byteoffset, <entire line>) from Record Reader
    // OUTPUT : ((friend1,friend2),([<array_friendlist>])) --> friend1.friend2 -> key &[<array_friendlist>] ->value
    // KEY VALUE PAIRS ARE SEPERATED BY TAB("\t") IN BACKEND
    public static class MyMap1 
    extends Mapper<LongWritable,Text,Text,Text>
    {
        private Text output =new Text();
        public void map(LongWritable key,Text value,Context context) throws InterruptedException,IOException
        {
            String[] input_array = value.toString().split("\t");
            String user = input_array[0];
            if (input_array.length == 2) // to ensure that input is of correct format
            {
                String[] split_friends = input_array[1].split(",");
                List<String> friends_list = Arrays.asList(split_friends);
                for (String friend : friends_list)
                {
                    int user_id = Integer.parseInt(user);
                    int friend_id = Integer.parseInt(friend);
                    //sorting smaller ids on the leftside to avoid missing duplicates during reducing stage
                    // (friend1, friend2) is same as (friend2, friend1)
                    if (user_id < friend_id)
                    {
                        output.set(user+","+friend);
                    }
                    else
                    {
                        output.set(friend+","+user);
                    }
                    context.write(output,new Text(input_array[1]));
                }
            }
        }
    }

    // Method will form pairs of friends such that smaller id is on the left side
    // INPUT:  ((friend1,friend2),([<array_friendlist>])) from 1st Mapper
    // OUTPUT : ((friend1,friend2),(count_mutual_friends#)) --> friend1.friend2 -> key & count_mutual_friends# ->value
    public static class MyReduce1 
    extends Reducer<Text,Text,Text,IntWritable>
    {
        public void reduce (Text key, Iterable<Text> values, Context context) throws InterruptedException,IOException
        {
            int count_MutualFriends =0;
            HashMap<String,Integer> hash_map = new HashMap<String, Integer>();
            for(Text value : values)
            {
                List<String> friends = Arrays.asList(value.toString().split(","));
                for (String friend : friends)
                {
                    if (hash_map.containsKey(friend))
                    {
                        count_MutualFriends+=1;
                    }
                    else
                    {
                        hash_map.put(friend,1);
                    }
                }
            }
            context.write(key,new IntWritable(count_MutualFriends));

        }
    }


    //Sending all the values to one mapper which'll be sent to one reducer later
    // O/P of above reducer will go to below mapper through Record Reader which will read lines one by one
    // File input format is set to TextInputFormat - so this Record Reader will form key value pairs as :
    // (byteoffset, <entire line>). So mapper will have f1,f2 count# in the VALUES
    //INPUT : ((byteoffset,friend1,friend2),(count_mutual_friends#)) --> byteoffset->key & friend1,friend2\tcount_mutual_friends# is value
    //OUTPUT : ((1),(friend1,friend2\tcount_mutual_friends#)) --> 1 ->key & rest is value
    // KEY VALUE PAIRS ARE SEPERATED BY TAB("\t") IN BACKEND
    public static class MyMap2 extends Mapper<LongWritable, Text, IntWritable, Text>
    {
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(one, value);
        }
    }

    //Add all friends pairs with count of their mutual friends to a map and then sort them in descending order
    //INPUT : ((1),(friend1,friend2\tcount_mutual_friends#))--> 1->key &
    // KEY VALUE PAIRS ARE SEPERATED BY TAB("\t") IN BACKEND
    private static class MyReduce2 extends Reducer<IntWritable,Text,Text,IntWritable>
    {
        public void reduce (IntWritable key, Iterable<Text> values, Context context) throws InterruptedException,IOException
        {
            HashMap map = new HashMap<String,Integer>();
            for (Text val : values)
            {
                String[] data = val.toString().split("\t");
                String friend_pair_names = data[0];
                Integer count_MutualFriends = Integer.parseInt(data[1]);
                map.put(friend_pair_names,count_MutualFriends);
            }
            //sorting these values
            ValueComparator vc = new ValueComparator(map);
            TreeMap<String, Integer> sorted_map = new TreeMap<String,Integer>(vc);
            sorted_map.putAll(map);
            int remaining =10;
            for (Map.Entry<String,Integer> entry : sorted_map.entrySet())
            {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
                remaining--;
                // output only top 10
                if (remaining ==0)
                {
                    break;
                }

            }
        }
    }

    static class ValueComparator implements Comparator
    {
        HashMap map;
        public ValueComparator(HashMap map)
        {
            this.map = map;
        }
        @Override
        public int compare(Object key1, Object key2)
        {
            Integer a = (Integer) map.get(key1);
            Integer b = (Integer) map.get(key2);
            if(b >= a)
            {
                return 1;
            }
            else
            {
                return -1;
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config,args).getRemainingArgs();
        if (otherArgs.length !=3)
        {
            System.err.println("Usage: Q1 Mutual Friends <inputfile hdfs path> <output file1 hdfs path> <output file2 hdfs path>");
            System.exit(2);
        }
        Job job1 = Job.getInstance(config,"Q2MutualFriends");
        job1.setJarByClass(Q2_Top_10.class);
        job1.setMapperClass(MyMap1.class);
        job1.setReducerClass(MyReduce1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

        // Job chaining - sending output of 1st Reducer to 2nd Mapper

        if (job1.waitForCompletion(true))
        {
            Configuration config2 = new Configuration();
            Job job2 = Job.getInstance(config2, "Q2Top10");
            job2.setJarByClass(Q2_Top_10.class);
            job2.setMapperClass(MyMap2.class);
            job2.setReducerClass(MyReduce2.class);
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0:1);
        }

    }

}
