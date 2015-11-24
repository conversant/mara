package com.conversantmedia.mapreduce.tool.annotation;

/*
 * #%L
 * Mara Core framework
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


import javax.annotation.Resource;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

@SuppressWarnings("rawtypes")
public class TestMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@NamedOutput
	private MultipleOutputs<LongWritable,Text> multiOut1;

	@NamedOutput(name="second",type=@KeyValue(key=Text.class,value=Text.class))
	private MultipleOutputs multiOut2;

	@NamedOutput({"third0","third1"})
	private MultipleOutputs<IntWritable, NullWritable> multiOut3;

	@NamedOutput(name={"fourth0","fourth1"},type=@KeyValue(key=Text.class,value=NullWritable.class))
	private MultipleOutputs<Text, Text> multiOut4;

	// Try to add 'default' a second time. Should skip without exception
	@NamedOutput
	private MultipleOutputs<LongWritable,Text> multiOut5;

	@Resource
	private Path myPathResource;

	public Path getMyPathResource() {
		return this.myPathResource;
	}

	// For suppressing unused warnings
	// SuppressWarnings("unused") caused further warns.
	public void dummy() {
		multiOut1.hashCode();
		multiOut2.hashCode();
		multiOut3.hashCode();
		multiOut4.hashCode();
		multiOut5.hashCode();
	}
}

