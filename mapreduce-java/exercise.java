import java.io.* ;
import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

/*Sample Program that takes soccer players individual scores and computes total by player*/
public class exercise 
{
   
    public static class SoccerScoreMapper
    extends Mapper<Object, Text, Text, IntWritable>
{
       //Use map reduce specific data types 
        private final static IntWritable score = new IntWritable(1);
        private Text player = new Text();
        
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
	   //Read each line and split as player name and score
            String[] keyvalues = value.toString().split(",") ;

	    for ( String keyvalue : keyvalues ) {
			//System.out.println(keyvalue);

			String[] valueArray= keyvalue.split("=");

			if ( ( ! valueArray[0].equals("Game")) && 
				(! valueArray[0].equals("Date")) && 
				(! valueArray[0].equals("Goals")) ) {

				player.set(valueArray[0]);
				score.set(Integer.valueOf(valueArray[1]));
				context.write(player,score);
			}	
	    }
	    
        }
    }

public static class SoccerScoreReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
            Context context
            ) throws IOException, InterruptedException {
                int total = 0;
            for (IntWritable score : values) {
                total = total+score.get() ;
            }
            context.write(key, new IntWritable(total));
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Player scores");
        job.setJarByClass(exercise.class);
        job.setMapperClass(SoccerScoreMapper.class);
        job.setReducerClass(SoccerScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
