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

import javax.annotation.Resource;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.DriverContextBase;
import com.conversantmedia.mapreduce.tool.annotation.AvroJobInfo;
import com.conversantmedia.mapreduce.tool.annotation.Distribute;
import com.conversantmedia.mapreduce.tool.annotation.Driver;
import com.conversantmedia.mapreduce.tool.annotation.DriverContext;
import com.conversantmedia.mapreduce.tool.annotation.FileInput;
import com.conversantmedia.mapreduce.tool.annotation.FileOutput;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;
import com.conversantmedia.mapreduce.tool.annotation.MapperInfo;
import com.conversantmedia.mapreduce.tool.annotation.Option;
import com.dotomi.mapreduce.example.avro.AvroExample;

/**
 *
 *
 */
@Driver
public class AvroInputOutputExample {

	@DriverContext
	MinimumFrequencyContext context;

	@JobInfo(name="Demonstration of Avro inputs and outputs via annotation", numReducers="0")
	@MapperInfo(AvroWordFrequencyMapper.class)
	@FileInput(AvroKeyInputFormat.class)
	@FileOutput(AvroKeyOutputFormat.class)
	@AvroJobInfo(inputKeySchema=AvroExample.class,
				outputKeySchema=AvroExample.class)
	Job job;

	private static class MinimumFrequencyContext extends DriverContextBase {
		@Distribute
		@Option(required=true)
		private String minimum;
	}

	/**
	 *
	 */
	@Service
	public static class AvroWordFrequencyMapper extends
		Mapper<AvroKey<AvroExample>, NullWritable, AvroKey<AvroExample>, NullWritable> {

		@Resource
		private long minimum;

		private final AvroKey<AvroExample> _key = new AvroKey<AvroExample>();

		@Override
		protected void map(AvroKey<AvroExample> key, NullWritable value,
				Context context) throws IOException, InterruptedException {
			AvroExample record = key.datum();
			if (record.getFrequency() > minimum) {
				_key.datum(record);
				context.write(_key, NullWritable.get());
			}
		}
	}
}
