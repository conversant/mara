package com.conversantmedia.mapreduce.example.avro;

/*
 * #%L
 * Mara Framework Examples
 * ~~
 * Copyright (C) 2015 Conversant
 * ~~
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.annotation.AvroNamedOutput;
import com.dotomi.mapreduce.example.avro.AvroExample;

/**
 *
 *
 */
@Service
public class AvroWordCountReducer extends
		Reducer<Text, LongWritable, AvroKey<AvroExample>, NullWritable> {

	@AvroNamedOutput(record = AvroExample.class)
	private AvroMultipleOutputs avroMultiOut;

	private AvroKey<AvroExample> aKey = new AvroKey<AvroExample>();

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		AvroExample.Builder builder = AvroExample.newBuilder();
		builder.setWord(key.toString());
		builder.setFrequency(sum);
		AvroExample datum = builder.build();
		aKey.datum(datum);

		context.write(aKey, NullWritable.get());
		avroMultiOut.write(aKey, NullWritable.get(), "avroMulti");
	}

}
