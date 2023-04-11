package finance.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MinMaxMapper extends Mapper<Object , Text, Text, MinMaxWritable > {
    private final static MinMaxWritable min_max = new MinMaxWritable() ;
    private final Text word = new Text() ;

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        if(key.toString().equals("0")) return ;
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName().split("\\.")[0];
        word.set(fileName);
        String[] arr = value.toString().split(",");
        double closingPrice = Double.parseDouble(arr[5]);
        System.out.println(closingPrice);
        min_max.setMin(closingPrice);
        min_max.setMax(closingPrice);
        context.write(word,min_max);
    }
}
