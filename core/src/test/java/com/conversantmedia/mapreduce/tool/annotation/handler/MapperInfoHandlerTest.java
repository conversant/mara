package com.conversantmedia.mapreduce.tool.annotation.handler;

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


import static org.mockito.Mockito.times;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.conversantmedia.mapreduce.tool.annotation.handler.MapperInfoHandler;

public class MapperInfoHandlerTest {

	MapperInfoHandler handler;
	Job job;

	@Test
	public void testConfigureMapperIntText() {
		handler.configureOutputKeyValue(job, TestMapperIntText.class, null, false);
		verify(job).setMapOutputKeyClass(IntWritable.class);
		verify(job).setMapOutputValueClass(Text.class);
		verify(job, never()).setOutputKeyClass(IntWritable.class);
		verify(job, never()).setOutputValueClass(IntWritable.class);
	}

	@Test
	public void testConfigureMapperIntText_MapOnly() {
		handler.configureOutputKeyValue(job, TestMapperIntText.class, null, true);
		verify(job).setOutputKeyClass(IntWritable.class);
		verify(job).setOutputValueClass(Text.class);
	}
	
	@Test
	public void testConfigureMapperLongText() {
		handler.configureOutputKeyValue(job, TestMapperLongText.class, null, false);
		verify(job).setMapOutputKeyClass(LongWritable.class);
		verify(job).setMapOutputValueClass(Text.class);
		verify(job, never()).setOutputKeyClass(LongWritable.class);
		verify(job, never()).setOutputValueClass(Text.class);
	}

	@Test
	public void testConfigureMapperLongText_MapOnly() {
		handler.configureOutputKeyValue(job, TestMapperLongText.class, null, true);
		verify(job).setOutputKeyClass(LongWritable.class);
		verify(job).setOutputValueClass(Text.class);
	}
	
	@Test
	public void testConfigureMapperTextText() {
		handler.configureOutputKeyValue(job, TestMapperTextText.class, null, false);
		verify(job).setMapOutputKeyClass(Text.class);
		verify(job).setMapOutputValueClass(NullWritable.class);
		verify(job, never()).setOutputKeyClass(Text.class);
		verify(job, never()).setOutputValueClass(NullWritable.class);
	}
	
	@Test
	public void testConfigureMapperTextText_MapOnly() {
		handler.configureOutputKeyValue(job, TestMapperTextText.class, null, true);
		verify(job).setMapOutputKeyClass(Text.class);
		verify(job).setMapOutputValueClass(NullWritable.class);
		verify(job).setOutputKeyClass(Text.class);
		verify(job).setOutputValueClass(NullWritable.class);
	}

	@Test
	public void testConfigureMapperSubclassing() {
		handler.configureOutputKeyValue(job, TestMapperSubclassTest.class, null, false);
		verify(job).setMapOutputKeyClass(Text.class);
		verify(job).setMapOutputValueClass(NullWritable.class);
		verify(job, never()).setOutputKeyClass(Text.class);
		verify(job, never()).setOutputValueClass(NullWritable.class);
	}

	@Test
	public void testConfigureMapperSubclassing_MapOnly() {
		handler.configureOutputKeyValue(job, TestMapperSubclassTest.class, null, true);
		verify(job).setMapOutputKeyClass(Text.class);
		verify(job).setMapOutputValueClass(NullWritable.class);
		verify(job).setOutputKeyClass(Text.class);
		verify(job).setOutputValueClass(NullWritable.class);
	}

	@Test
	public void testConfigureMapperNoGenerics() {
		handler.configureOutputKeyValue(job, TestMapperNoGenericsTest.class, null, false);

		verify(job, never()).setMapOutputKeyClass(any(Class.class));
		verify(job, never()).setMapOutputValueClass(any(Class.class));
	}


	@Test
	public void testConfigureMapperTableMapper() {
		handler.configureOutputKeyValue(job, TestTableMapper.class, null, false);

		verify(job, times(1)).setMapOutputKeyClass(Text.class);
		verify(job, times(1)).setMapOutputValueClass(NullWritable.class);
		
		verify(job, never()).setOutputKeyClass(Text.class);
		verify(job, never()).setOutputValueClass(NullWritable.class);
	}

	@Test
	public void testConfigureMapperTableMapper_MapOnly() {
		handler.configureOutputKeyValue(job, TestTableMapper.class, null, true);

		verify(job, times(1)).setMapOutputKeyClass(Text.class);
		verify(job, times(1)).setMapOutputValueClass(NullWritable.class);
		
		verify(job).setOutputKeyClass(Text.class);
		verify(job).setOutputValueClass(NullWritable.class);
	}
	
	public static class TestMapperIntText extends Mapper<LongWritable, Text, IntWritable, Text> {}
	public static class TestMapperLongText extends Mapper<LongWritable, Text, LongWritable, Text> {}
	public static class TestMapperTextText extends Mapper<LongWritable, Text, Text, NullWritable> {}
	public static class TestMapperSubclassTest extends TestMapperTextText {}
	@SuppressWarnings("rawtypes")
	public static class TestMapperNoGenericsTest extends Mapper {}
	public static class TestTableMapper extends TableMapper<Text, NullWritable> {}

	@Before
	public void setup() {
		this.handler = new MapperInfoHandler();

		Configuration conf = new Configuration();
		this.job = mock(Job.class);
		when(job.getConfiguration()).thenReturn(conf);
	}

	@After
	public void cleanup() {
		this.job = null;
		this.handler= null;
	}
}
