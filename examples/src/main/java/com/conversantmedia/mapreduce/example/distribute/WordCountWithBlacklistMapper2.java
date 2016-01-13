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
import java.util.Set;
import java.util.StringTokenizer;

import javax.annotation.Resource;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.conversantmedia.mapreduce.tool.annotation.MapperService;

/**
 *
 *
 */
@MapperService
public class WordCountWithBlacklistMapper2 extends Mapper<LongWritable, Text, Text, LongWritable> {

	private final static LongWritable ONE = new LongWritable(1);

	@Resource(name="blacklist")
	private Set<String> blacklistedWords;

	private final Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			String nextWord = tokenizer.nextToken().replaceAll( "\\W", "" );
			if (!blacklistedWords.contains(nextWord)) {
				word.set(nextWord);
				context.write(word, ONE);
			}
		}
	}
}