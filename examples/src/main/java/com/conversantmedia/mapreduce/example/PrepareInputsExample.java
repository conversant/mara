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
import java.util.UUID;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.conversantmedia.mapreduce.tool.DriverContextBase;
import com.conversantmedia.mapreduce.tool.annotation.Driver;
import com.conversantmedia.mapreduce.tool.annotation.DriverCleanup;
import com.conversantmedia.mapreduce.tool.annotation.DriverContext;
import com.conversantmedia.mapreduce.tool.annotation.DriverInit;
import com.conversantmedia.mapreduce.tool.annotation.FileInput;
import com.conversantmedia.mapreduce.tool.annotation.FileOutput;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;
import com.conversantmedia.mapreduce.tool.annotation.MapperInfo;
import com.conversantmedia.mapreduce.tool.annotation.ReducerInfo;

/**
 * In this example we will do some pre-processing on the
 * inputs (copy to a 'working' directory) before kicking off the job.
 *
 *
 */
@Driver("prepare-inputs")
public class PrepareInputsExample extends Configured {

	@DriverContext
	private DriverContextBase context;

	@JobInfo(value="Annotated Word Count v1")
	@MapperInfo(WordCountMapper.class)
	@ReducerInfo(WordCountReducer.class)
	// 'property' specifies driver property containing the input value (a path in the case of file input)
	@FileInput(value=TextInputFormat.class, path="${workingDirectory}")
	@FileOutput(TextOutputFormat.class)
	private Job job;

	private Path workingDirectory;

	@DriverInit
	public void copyFilesToWorking() throws IOException {
		// Copy the input files into the 'workingDir'
		FileSystem fs = FileSystem.get(getConf());

		this.workingDirectory = new Path("/tmp/" + UUID.randomUUID().toString());
		fs.mkdirs(workingDirectory);

		FileStatus[] files = fs.globStatus(new Path(context.getInput()));
		for (FileStatus file : files) {
			Path dest = new Path(workingDirectory, file.getPath().getName());
			FileUtil.copy(fs, file.getPath(), fs, dest, false, getConf());
		}
	}

	@DriverCleanup
	public void cleanup() throws IOException {
		FileSystem fs = FileSystem.get(getConf());
		fs.deleteOnExit(getWorkingDirectory());
	}

	public Path getWorkingDirectory() {
		return this.workingDirectory;
	}

}
