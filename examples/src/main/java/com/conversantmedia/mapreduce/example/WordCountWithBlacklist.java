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


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.conversantmedia.mapreduce.tool.DriverContextBase;
import com.conversantmedia.mapreduce.tool.annotation.Distribute;
import com.conversantmedia.mapreduce.tool.annotation.Driver;
import com.conversantmedia.mapreduce.tool.annotation.DriverContext;
import com.conversantmedia.mapreduce.tool.annotation.FileInput;
import com.conversantmedia.mapreduce.tool.annotation.FileOutput;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;
import com.conversantmedia.mapreduce.tool.annotation.MapperInfo;
import com.conversantmedia.mapreduce.tool.annotation.Option;
import com.conversantmedia.mapreduce.tool.annotation.ReducerInfo;

@Driver("wordcount-blacklist")
public class WordCountWithBlacklist {

	@DriverContext
	private BlacklistContext context;

	@JobInfo("Distributed Cache Example Job")
	@MapperInfo(WordCountWithBlacklistMapper.class)
	@ReducerInfo(WordCountReducer.class)
	@FileInput(TextInputFormat.class)
	@FileOutput(TextOutputFormat.class)
	Job job;

	@Distribute
	public Path getBlacklist() {
		if (context.blacklist != null) {
			return new Path(context.blacklist);
		}
		return null;
	}

	public static class BlacklistContext extends DriverContextBase {
		@Option(argName="path", description="Blacklist to apply to the wordcount.")
		private String blacklist;
	}
}
