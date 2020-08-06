import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.List;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Iterator;
import java.io.IOException;

public class Q1MutualFriends
{
    public static class MyMap 
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

    public static class MyReduce 
    extends Reducer<Text,Text,Text,Text>
    {
        public void reduce (Text key, Iterable<Text> values, Context context) throws InterruptedException,IOException
        {
            String mutual_friends = "";
            List<String> mutual_friends_list = new LinkedList<>();
            Text[] data = new Text[2];
            Iterator<Text> iterator = values.iterator();
            int index =0;
            while (iterator.hasNext())
            {
                data[index++] = new Text(iterator.next());
            }
            String[] friend_list_user =data[0].toString().split(",");
            String[] friend_list_friend = data[1].toString().split(",");
            for(String frnd1 : friend_list_user)
            {
                for (String frnd2 : friend_list_friend)
                {
                    if(frnd1.equals(frnd2))
                    {
                        mutual_friends_list.add(frnd1);
                    }
                }
            }
            for (int i=0; i<mutual_friends_list.size();i++)
            {
                if(i != mutual_friends_list.size() -1)
                {
                    mutual_friends += mutual_friends_list.get(i) + ",";
                }
                else
                {
                    mutual_friends += mutual_friends_list.get(i); // last entry doesn't end with a ,
                }
            }
            context.write(key,new Text(mutual_friends));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config,args).getRemainingArgs(); //get all args
        if (otherArgs.length !=2) {
            System.err.println("Usage: Q1 Mutual Friends <inputfile hdfs path> <output file hdfs path>");
            System.exit(2);
        }
        Job job = new Job(config,"Q1MutualFriends");
        job.setJarByClass(Q1MutualFriends.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
