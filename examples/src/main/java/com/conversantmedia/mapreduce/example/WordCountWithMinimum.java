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


import org.apache.hadoop.mapreduce.Job;

import com.conversantmedia.mapreduce.tool.DriverContextBase;
import com.conversantmedia.mapreduce.tool.annotation.Distribute;
import com.conversantmedia.mapreduce.tool.annotation.Driver;
import com.conversantmedia.mapreduce.tool.annotation.DriverContext;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;
import com.conversantmedia.mapreduce.tool.annotation.MapperInfo;
import com.conversantmedia.mapreduce.tool.annotation.Option;
import com.conversantmedia.mapreduce.tool.annotation.ReducerInfo;

@Driver("wordcount-minimum")
public class WordCountWithMinimum {

	@DriverContext
	private MinimumWordCountContext context;

	@JobInfo("Another Distributed Cache Example Job")
	@MapperInfo(WordCountMapper.class)
	@ReducerInfo(WordCountReducerWithMinimum.class)
	Job job;

	public static class MinimumWordCountContext extends DriverContextBase {
		@Option(argName="value", description="Minimum number of times a word must appear before it is included.")
		@Distribute
		private String minimum;
	}
}
