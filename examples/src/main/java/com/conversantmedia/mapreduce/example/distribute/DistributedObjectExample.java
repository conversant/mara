package com.conversantmedia.mapreduce.example.distribute;

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
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.conversantmedia.mapreduce.example.WordCountReducerWithMinimum;
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

@Driver(value="distributed-blacklist",
description="Demonstrates distributing an object resource to the M/R framework components.")
public class DistributedObjectExample {

	@DriverContext
	private WordcountContext context;

	@JobInfo("Distributed Cache Example Job. Miminum ${context.minimum}")
	@MapperInfo(WordCountWithBlacklistMapper2.class)
	@ReducerInfo(WordCountReducerWithMinimum.class)
	@FileInput(TextInputFormat.class)
	@FileOutput(TextOutputFormat.class)
	Job job;

	/**
	 * Distributed {@link Set} containing the prepared list of blacklisted words.
	 * 
	 * @return Set			the set of blacklisted words
	 * @throws IOException	if it fails to read in the file
	 */
	@Distribute
	public Set<String> getBlacklist() throws IOException {
		Set<String> blacklist = null;
		if (StringUtils.isNotBlank(context.blacklist)) {
			blacklist = new HashSet<>();
			InputStreamReader reader = null;
			try {
				FileSystem fs = FileSystem.get(new Configuration());
				FileStatus file = fs.getFileStatus(new Path(context.blacklist));
				reader = new InputStreamReader(fs.open(file.getPath()));
				for (String line : IOUtils.readLines(reader)) {
					blacklist.add(line);
				}
			}
			finally {
				IOUtils.closeQuietly(reader);
			}
		}
		return blacklist;
	}

	/**
	 * Custom context.
	 */
	public static class WordcountContext extends DriverContextBase {
		@Option(required=true)
		@Distribute
		private String minimum;

		@Option(required=true, argName="local-file", description="Blacklist to apply to the wordcount.")
		private String blacklist;

		public String getMinimum() {
			return this.minimum;
		}
	}
}
