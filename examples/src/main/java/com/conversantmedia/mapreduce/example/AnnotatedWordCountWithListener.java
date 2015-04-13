package com.conversantmedia.mapreduce.example;

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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.conversantmedia.mapreduce.example.AnnotatedWordCountWithListener.WordCountListener;
import com.conversantmedia.mapreduce.tool.DriverContextBase;
import com.conversantmedia.mapreduce.tool.annotation.Driver;
import com.conversantmedia.mapreduce.tool.annotation.DriverContext;
import com.conversantmedia.mapreduce.tool.annotation.FileInput;
import com.conversantmedia.mapreduce.tool.annotation.FileOutput;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;
import com.conversantmedia.mapreduce.tool.annotation.MapperInfo;
import com.conversantmedia.mapreduce.tool.annotation.ReducerInfo;
import com.conversantmedia.mapreduce.tool.event.ToolEvent;
import com.conversantmedia.mapreduce.tool.event.ToolListener;

@Driver(value="annotated-wordcount-listener", listener=WordCountListener.class)
public class AnnotatedWordCountWithListener {

	@DriverContext
	private DriverContextBase context;

	@JobInfo(value="Annotated Word Count with Listener")
	@MapperInfo(WordCountMapper.class)
	@ReducerInfo(WordCountReducer.class)
	@FileInput(TextInputFormat.class)
	@FileOutput(TextOutputFormat.class)
	private Job job;

	public static class WordCountListener implements ToolListener<DriverContextBase> {

		@Override
		public void onAfterInitCliOptions(ToolEvent<DriverContextBase> event,
				Options options) throws Exception {
			System.out.println("After CLI Options initialization [" + event.getContext() + "," + options.toString() + "]");
		}

		@Override
		public void onAfterParseCli(ToolEvent<DriverContextBase> event,
				CommandLine line) throws Exception {
			System.out.println("After Parse Command Line [" + event.getContext() + "," + line.toString() + "]");
		}

		public void onBeforeInitDriver(String uid, ToolEvent<DriverContextBase> event) throws Exception {
			System.out.println("Before Driver Initialization [ uid = " + uid + ", " + event.getContext() + "]");
		}

		@Override
		public void onBeforeInitJob(ToolEvent<DriverContextBase> event) throws Exception {
			System.out.println("Before Job Initialization [" + event.getContext() + "]");
		}

		@Override
		public void onBeforeMapReduceSubmit(ToolEvent<DriverContextBase> event)
				throws Exception {
			System.out.println("Before MapReduce Submit [" + event.getContext() + "]");
		}

		@Override
		public void onAfterMapReduceFinish(int retval, ToolEvent<DriverContextBase> event)
				throws Exception {
			System.out.println("After MapReduce Finish [ retval = " + retval + ", " + event.getContext() + "]");
		}

		@Override
		public void onBeforeExit(ToolEvent<DriverContextBase> event) throws Exception {
			System.out.println("Before Driver Exit [" + event.getContext() + "]");
		}

		@Override
		public void onJobException(ToolEvent<DriverContextBase> event, Exception e)
				throws Exception {
			System.out.println("Job Exception [" + event.getContext() + "]");
			e.printStackTrace();
		}

	}
}
