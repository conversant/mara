package com.conversantmedia.mapreduce.example.hbase;

import com.conversantmedia.mapreduce.tool.DriverContextBase;
import com.conversantmedia.mapreduce.tool.annotation.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

@Driver("hbase-input")
public class HBaseInputJob {

    @DriverContext
    @Distribute
    HBaseContext context;

    @JobInfo(numReducers = "0")
    @MapperInfo(ExampleMapper.class)
    @TableInput
    Job job;

    public static class HBaseContext extends DriverContextBase implements java.io.Serializable {
        @Option(shortName = "c", required = true) String colfamily;
        @Option(shortName = "q", required = true) String qualifier;
    }

    @MapperService
    public static class ExampleMapper extends TableMapper<Text, Text> {

        Text _key = new Text();
        Text _value = new Text();

        @Resource(name="context")
        HBaseContext driverContext;

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {

            // We'll just convert the rowkey to text and write it out
            _key.set(new String(key.get()));

            // our values
            List<KeyValue> keyValueList = value.getColumn(driverContext.colfamily.getBytes(), driverContext.qualifier.getBytes());
            for (KeyValue kv : keyValueList) {
                _value.set(new String(kv.getValue()));
            }

            // write it out
            context.write(_key, _value);
        }
    }

}
