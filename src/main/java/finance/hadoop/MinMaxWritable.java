package finance.hadoop ;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class MinMaxWritable implements Writable {
    private double min ;
    private double max ;
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(min);
        dataOutput.writeDouble(max);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        min = dataInput.readDouble();
        max = dataInput.readDouble();

    }

    @Override
    public String toString() {
        return "MinMaxWritable{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }
}