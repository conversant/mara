package com.conversantmedia.mapreduce.tool.annotation.handler;

import java.lang.annotation.Annotation;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.FileInput;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;

/**
 * This is a special handler designed to setup a default 
 * {@link InputFormat} for the job if none is specified.
 * 
 */
public class DefaultInputAnnotationHandler extends FileInputAnnotationHandler {

	@Override
	public boolean accept(Annotation annotation) {
		return annotation.annotationType() == JobInfo.class; // Only 1x. Use required JobInfo annotation as trigger
	}

	@Override
	public void process(Annotation annotation, Job job, Object target) throws ToolException {
		// If the job doesn't have an input format specified, we'll 
		// establish our default here.
		try {
			if (TextInputFormat.class.equals(job.getInputFormatClass()) // Hadoop default
					&& StringUtils.isBlank(job.getConfiguration().get(FileInputFormat.INPUT_DIR))) {
				annotation = DefaultInput.class.getDeclaredField("job").getAnnotation(FileInput.class);
				super.process(annotation, job, null);
			}
		} catch (ClassNotFoundException | NoSuchFieldException | SecurityException e) {
			throw new IllegalStateException(e);
		}
	}

	// For the default annotation
	private static final class DefaultInput {
		@FileInput(TextInputFormat.class)
		Job job;
	}
}
