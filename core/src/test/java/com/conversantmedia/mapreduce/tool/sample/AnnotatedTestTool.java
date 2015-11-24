package com.conversantmedia.mapreduce.tool.sample;

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


import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.conversantmedia.mapreduce.tool.annotation.Distribute;
import com.conversantmedia.mapreduce.tool.annotation.Driver;
import com.conversantmedia.mapreduce.tool.annotation.DriverCleanup;
import com.conversantmedia.mapreduce.tool.annotation.DriverContext;
import com.conversantmedia.mapreduce.tool.annotation.DriverInit;
import com.conversantmedia.mapreduce.tool.annotation.FileInput;
import com.conversantmedia.mapreduce.tool.annotation.FileOutput;
import com.conversantmedia.mapreduce.tool.annotation.Grouping;
import com.conversantmedia.mapreduce.tool.annotation.Hidden;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;
import com.conversantmedia.mapreduce.tool.annotation.JobInit;
import com.conversantmedia.mapreduce.tool.annotation.KeyValue;
import com.conversantmedia.mapreduce.tool.annotation.MapperInfo;
import com.conversantmedia.mapreduce.tool.annotation.Partitioner;
import com.conversantmedia.mapreduce.tool.annotation.ReducerInfo;
import com.conversantmedia.mapreduce.tool.annotation.Sorter;
import com.conversantmedia.mapreduce.tool.event.ToolEvent;
import com.conversantmedia.mapreduce.tool.event.ToolListener;

@Driver(value="sampleTool",
		description="A tool for testing and playing with annotations.",
		listener={com.conversantmedia.mapreduce.tool.sample.AnnotatedTestTool.TestToolListener.class, 
					com.conversantmedia.mapreduce.tool.sample.AnnotatedTestTool.TestToolListener2.class})
@Hidden
public class AnnotatedTestTool  {

	@DriverContext
	private TestToolContext context;

	@JobInfo(value = "Annotated Test  Job", numReducers = "10")
	@MapperInfo(TestMapper.class)
	@ReducerInfo(value=TestReducer.class, output=@KeyValue(key=Text.class, value=LongWritable.class))
	@FileInput(TextInputFormat.class)
	@FileOutput(TextOutputFormat.class)
	@Sorter(WritableComparator.class)
	@Grouping(WritableComparator.class)
	@Partitioner(TestPartitioner.class)
	private Job job;

	@DriverInit
	public void init() {
		// Can do extra stuff here now if we want...
		System.out.println("Context = " + this.context);
		System.out.println("Input path is: " + this.context.getInput());
		System.out.println("Output path is: " + this.context.getOutput());
		System.out.println("Archive path is: " + this.context.getArchive());
		System.out.println("My Property = " + this.context.getMyProperty());
	}

	@JobInit
	public void initJob() {
		System.out.println("Job = " + job);
	}

	@DriverCleanup
	public void cleanUp() {
		System.out.println("Cleaning up tool.");
	}

	@Distribute
	public String getMyStringResource() {
		return "Hello, World!";
	}

	public static class TestToolListener2 extends TestToolListener{	}

	public static class TestToolListener implements ToolListener<TestToolContext> {

		@Override
		public void onAfterInitCliOptions(ToolEvent<TestToolContext> event,
				Options options) throws Exception {
			System.out.println("After CLI Initialization.");
		}

		@Override
		public void onAfterParseCli(ToolEvent<TestToolContext> event, CommandLine line) throws Exception {
			System.out.println("After CLI Parsing.");
		}

		@Override
		public void onBeforeInitDriver(String id, ToolEvent<TestToolContext> event)
				throws Exception {
			System.out.println("Before Driver initialization: " + id + " -- " + event.getContext().getInput() + " -- " + event.getContext().getOutput());
		}

		@Override
		public void onBeforeInitJob(ToolEvent<TestToolContext> event)
				throws Exception {
			System.out.println("Before Job initialization: " + event.getContext().getInput() + " -- " + event.getContext().getOutput());
		}

		@Override
		public void onBeforeMapReduceSubmit(ToolEvent<TestToolContext> event)
				throws Exception {
			System.out.println("Before Submitting MR Job");
		}

		@Override
		public void onAfterMapReduceFinish(int retVal, ToolEvent<TestToolContext> event)
				throws Exception {
			System.out.println("onAfterMapReduceFinish");
		}

		@Override
		public void onBeforeExit(ToolEvent<TestToolContext> event)
				throws Exception {
			System.out.println("onBeforeExit");
		}

		@Override
		public void onJobException(ToolEvent<TestToolContext> event,
				Exception e) throws Exception {
			System.out.println("onJobException");
		}

	}

	public static class TestMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			super.map(key, value, context);
		}

	}

	public static class TestReducer extends Reducer<Text, LongWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context)
				throws IOException, InterruptedException {
			super.reduce(key, values, context);
		}
	}

	@SuppressWarnings("rawtypes")
	public static class TestPartitioner extends org.apache.hadoop.mapreduce.Partitioner {

		@Override
		public int getPartition(Object key, Object value, int numPartitions) {
			return 0;
		}

	}
}
