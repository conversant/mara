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


import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.conversantmedia.mapreduce.example.WordCountMapper;
import com.conversantmedia.mapreduce.tool.annotation.AvroJobInfo;
import com.conversantmedia.mapreduce.tool.annotation.AvroNamedOutput;
import com.conversantmedia.mapreduce.tool.annotation.Driver;
import com.conversantmedia.mapreduce.tool.annotation.FileInput;
import com.conversantmedia.mapreduce.tool.annotation.FileOutput;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;
import com.conversantmedia.mapreduce.tool.annotation.MapperInfo;
import com.conversantmedia.mapreduce.tool.annotation.ReducerInfo;
import com.dotomi.mapreduce.example.avro.AvroExample;

/**
 * This is an example job that writes output to an
 * {@link AvroNamedOutput}.
 *
 */
@Driver
public class AvroMultipleOutputExample {

	@JobInfo("Demonstration of Avro named outputs via annotation")
	@MapperInfo(WordCountMapper.class)
	@ReducerInfo(AvroWordCountReducer.class)
	@FileInput(TextInputFormat.class)
	@FileOutput(AvroKeyOutputFormat.class)
	@AvroJobInfo(outputKeySchema=AvroExample.class)
	Job job;

}
