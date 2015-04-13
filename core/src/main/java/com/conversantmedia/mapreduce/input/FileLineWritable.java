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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * Slightly modified copy of (reviewed and understood) code from:
 * http://www.idryman.org/blog/2013/09/22/process-small-files-on-hadoop-using-
 * combinefileinputformat-1/
 *
 *
 */
public class FileLineWritable implements WritableComparable<FileLineWritable> {
	public long offset;
	public String fileName;

	public FileLineWritable() {}

	public FileLineWritable(String fileName, Long offset) {
		this.fileName = fileName;
		this.offset = offset;
	}

	public long getOffset() {
		return offset;
	}

	public String getFileName() {
		return fileName;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.offset = in.readLong();
		this.fileName = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(offset);
		Text.writeString(out, fileName);
	}

	@Override
	public int compareTo(FileLineWritable that) {
		int cmp = this.fileName.compareTo(that.fileName);
		if (cmp != 0) {
			return cmp;
		}
		return (int) Math.signum((double) (this.offset - that.offset));
	}

	@Override
	public int hashCode() { // generated hashCode()
		final int prime = 31;
		int result = 1;
		result = prime * result + (fileName == null ? 0 : fileName.hashCode());
		result = prime * result + (int) (offset ^ offset >>> 32);
		return result;
	}

	@Override
	public boolean equals(Object obj) { // generated equals()
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		FileLineWritable other = (FileLineWritable) obj;
		if (fileName == null) {
			if (other.fileName != null) {
				return false;
			}
		} else if (!fileName.equals(other.fileName)) {
			return false;
		}
		if (offset != other.offset) {
			return false;
		}
		return true;
	}

}