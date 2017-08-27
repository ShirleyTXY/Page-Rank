import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
            String[] strs = value.toString().trim().split("\t");
            if (strs.length < 2) {
                return;
            }
            String outputKey = strs[0];
            String[] tos = strs[1].split(",");
            int count = tos.length;

            for (String to : tos) {
                context.write(new Text(outputKey), new Text(to + "=" + (double)1 / count));
               // context.getCounter("PRValue", "finish transition" + to + "=" + (double)1 / count);
            }

        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
            String[] strs = value.toString().trim().split("\t");
            if (strs.length < 2) {
                return;
            }
            String outputKey = strs[0];
            String outputValue = strs[1];
            context.write(new Text(outputKey), new Text(outputValue));
            //context.getCounter("PRValue", "Finish PR Mapper" + outputValue);
        }

    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            List<String> toCount = new ArrayList<String>();
            double pk = 1;
            for (Text text : values) {
                String str = text.toString().trim();
                context.getCounter("PRValue", str);
                if (str.indexOf("=") == -1) {
                    pk = Double.parseDouble(str);
                    //context.getCounter("PRValue", String.valueOf(pk));
                } else {
                    toCount.add(str);
                }
            }

            for (String text : toCount) {
                String[] strs = text.split("=");
                if(strs.length == 2) {
                    String to = strs[0];
                    double relation = Double.parseDouble(strs[1]);
                    context.write(new Text(to), new Text(String.valueOf(relation * pk)));
                   // context.getCounter("PRValue", to + String.valueOf(relation * pk));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?

        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
