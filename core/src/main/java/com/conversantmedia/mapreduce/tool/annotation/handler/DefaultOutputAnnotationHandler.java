package com.conversantmedia.mapreduce.tool.annotation.handler;

import java.lang.annotation.Annotation;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.FileOutput;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;
import org.springframework.stereotype.Service;

/**
 * This is a special handler designed to setup a default 
 * {@link OutputFormat} for the job if none is specified.
 * 
 */
@Service
public class DefaultOutputAnnotationHandler extends FileOutputAnnotationHandler {

	@Override
	public boolean accept(Annotation annotation) {
		return annotation.annotationType() == JobInfo.class; // Only 1x. Use required JobInfo annotation as trigger 
	}

	@Override
	public boolean runLast() {
		return true;
	}

	@Override
	public void process(Annotation annotation, Job job, Object target) throws ToolException {
		// If the job doesn't have an output format specified, we'll 
		// establish our default here.
		try {
			if (TextOutputFormat.class.equals(job.getOutputFormatClass())
					&& StringUtils.isBlank(job.getConfiguration().get("mapred.output.dir"))) {
				annotation = DefaultOutput.class.getDeclaredField("job").getAnnotation(FileOutput.class);
				super.process(annotation, job, null);
			}
		} catch (ClassNotFoundException | NoSuchFieldException | SecurityException e) {
			throw new IllegalStateException(e);
		}
	}

	// For the default annotation
	private static final class DefaultOutput {
		@FileOutput(TextOutputFormat.class)
		Job job;
	}
}
