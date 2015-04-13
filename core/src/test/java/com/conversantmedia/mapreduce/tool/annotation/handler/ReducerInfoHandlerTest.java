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


import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.conversantmedia.mapreduce.tool.annotation.handler.ReducerInfoHandler;

public class ReducerInfoHandlerTest {

	ReducerInfoHandler handler;
	Job job;

	@Test
	public void testConfigureReducerIntText() {
		handler.configureOutputKeyValue(job, TestReducerIntText.class, null);
		verify(job).setOutputKeyClass(IntWritable.class);
		verify(job).setOutputValueClass(Text.class);
	}

	@Test
	public void testConfigureReducerLongText() {
		handler.configureOutputKeyValue(job, TestReducerLongText.class, null);
		verify(job).setOutputKeyClass(LongWritable.class);
		verify(job).setOutputValueClass(Text.class);
	}

	@Test
	public void testConfigureReducerTextText() {
		handler.configureOutputKeyValue(job, TestReducerTextText.class, null);
		verify(job).setOutputKeyClass(Text.class);
		verify(job).setOutputValueClass(Text.class);
	}

	@Test
	public void testConfigureReducerSubclassing() {
		handler.configureOutputKeyValue(job, TestReducerSubclassTest.class, null);
		verify(job).setOutputKeyClass(Text.class);
		verify(job).setOutputValueClass(Text.class);
	}

	@Test
	public void testConfigureReducerNoGenerics() {
		handler.configureOutputKeyValue(job, TestReducerNoGenericsTest.class, null);
		verify(job, never()).setOutputKeyClass(any(Class.class));
		verify(job, never()).setOutputValueClass(any(Class.class));
	}

	public static class TestReducerIntText extends Reducer<LongWritable, Text, IntWritable, Text> {}
	public static class TestReducerLongText extends Reducer<LongWritable, Text, LongWritable, Text> {}
	public static class TestReducerTextText extends Reducer<LongWritable, Text, Text, Text> {}
	public static class TestReducerSubclassTest extends TestReducerTextText {}
	@SuppressWarnings("rawtypes")
	public static class TestReducerNoGenericsTest extends Reducer {}

	@Before
	public void setup() {
		this.handler = new ReducerInfoHandler();

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
