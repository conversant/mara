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
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Classic MapReduce 'WordCount' Driver (new API).
 *
 */
public class WordCount {

	public static void main(String[] args) {

		try {
			Job job = Job.getInstance(new Configuration(), "WordCount v2");

			job.setInputFormatClass(FileInputFormat.class);
			job.setOutputFormatClass(FileOutputFormat.class);

			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			job.setCombinerClass(WordCountReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);

		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}
