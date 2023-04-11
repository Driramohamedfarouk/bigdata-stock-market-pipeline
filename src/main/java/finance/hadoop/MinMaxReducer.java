package finance.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinMaxReducer extends Reducer<Text,MinMaxWritable,Text,MinMaxWritable> {
    private MinMaxWritable result = new MinMaxWritable();
    public void reduce(Text key, Iterable<MinMaxWritable> values,Context context) throws IOException, InterruptedException {
        System.out.println("Started Reducer "+key.toString());
        result.setMax(Double.NEGATIVE_INFINITY);
        result.setMin(Double.POSITIVE_INFINITY);
        for (MinMaxWritable val : values){
            if(result.getMin()> val.getMin()){
                result.setMin(val.getMin());
            }
            if(result.getMax()< val.getMax()){
                result.setMax(val.getMax());
            }
        }
        context.write(key,result);
    }
}
