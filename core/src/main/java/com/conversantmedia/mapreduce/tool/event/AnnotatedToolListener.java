package com.conversantmedia.mapreduce.tool.event;

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


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.conversantmedia.mapreduce.tool.AnnotatedToolContext;
import com.conversantmedia.mapreduce.tool.event.ToolEvent;
import com.conversantmedia.mapreduce.tool.event.ToolListener;

/**
 * Wraps the annotation-specified listener and adapt between the
 * {@link AnnotatedToolContext} and the caller's own context bean.
 *
 *
 */
public class AnnotatedToolListener implements ToolListener<AnnotatedToolContext> {

	private ToolListener<Object> delegate;

	public AnnotatedToolListener(ToolListener<Object> delegate) {
		this.delegate = delegate;
	}

	@Override
	public void onAfterInitCliOptions(ToolEvent<AnnotatedToolContext> event, Options options) throws Exception {
		delegate.onAfterInitCliOptions(new AnnotatedToolEvent(event), options);
	}

	@Override
	public void onAfterParseCli(ToolEvent<AnnotatedToolContext> event, CommandLine line) throws Exception {
		delegate.onAfterParseCli(new AnnotatedToolEvent(event), line);
	}

	@Override
	public void onBeforeInitDriver(String id, ToolEvent<AnnotatedToolContext> event) throws Exception {
		delegate.onBeforeInitDriver(id, new AnnotatedToolEvent(event));
	}

	@Override
	public void onBeforeInitJob(ToolEvent<AnnotatedToolContext> event) throws Exception {
		delegate.onBeforeInitJob(new AnnotatedToolEvent(event));
	}

	@Override
	public void onBeforeMapReduceSubmit(ToolEvent<AnnotatedToolContext> event)
			throws Exception {
		delegate.onBeforeMapReduceSubmit(new AnnotatedToolEvent(event));
	}

	@Override
	public void onAfterMapReduceFinish(int retVal, ToolEvent<AnnotatedToolContext> event)
			throws Exception {
		delegate.onAfterMapReduceFinish(retVal, new AnnotatedToolEvent(event));
	}

	@Override
	public void onBeforeExit(ToolEvent<AnnotatedToolContext> event) throws Exception {
		delegate.onBeforeExit(new AnnotatedToolEvent(event));
	}

	@Override
	public void onJobException(ToolEvent<AnnotatedToolContext> event, Exception e)
			throws Exception {
		delegate.onJobException(new AnnotatedToolEvent(event), e);
	}
}
