package com.conversantmedia.mapreduce.input;

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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Simple implementation of a combinefile input format.
 *
 * Concept for FileLineWritable borrowed from:
 * http://www.idryman.org/blog/2013/09/22/process-small-files-on-hadoop-using-combinefileinputformat-1/
 * Needed to rewrite to work properly in new API
 *
 */
public class CombineTextFileInputFormat extends CombineFileInputFormat<FileLineWritable, Text> {

	@Override
	public RecordReader<FileLineWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new CombineFileRecordReader<>(
				(CombineFileSplit)split, context,FileLineWritableRecordReader.class);
	}

	public static class FileLineWritableRecordReader extends RecordReader<FileLineWritable, Text> {

		private final LineRecordReader delegate;

		private String fileName;

		private final int splitIndex;

		public FileLineWritableRecordReader(CombineFileSplit split,
				TaskAttemptContext context, Integer splitIndex) {
			delegate = new LineRecordReader();
			this.splitIndex = splitIndex;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			CombineFileSplit combineSplit = (CombineFileSplit)split;
			Path path = combineSplit.getPath(splitIndex);
			this.fileName = path.getName();
			FileSplit fileSplit = new FileSplit(
					path,
					combineSplit.getOffset(splitIndex),
					combineSplit.getLength(splitIndex),
					combineSplit.getLocations());
			delegate.initialize(fileSplit, context);

		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return delegate.nextKeyValue();
		}

		@Override
		public FileLineWritable getCurrentKey() throws IOException,
				InterruptedException {
			LongWritable offset = delegate.getCurrentKey();
			return new FileLineWritable(this.fileName, offset.get());
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return delegate.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return delegate.getProgress();
		}

		@Override
		public void close() throws IOException {
			delegate.close();
		}

	}
}
