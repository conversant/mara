package com.conversantmedia.mapreduce.tool.event;

/*
 * #%L
 * Mara Annotations/API
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


/**
 * Default base do-nothing listener to ease the burden on subclasses.
 *
 * @param <T> the driver context bean type
 */
public class ToolListenerAdapter<T> implements ToolListener<T> {

	@Override
	public void onAfterInitCliOptions(ToolEvent<T> event, Options options) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public void onAfterParseCli(ToolEvent<T> event, CommandLine line) throws Exception {
		// do-nothing
	}

	@Override
	public void onBeforeInitDriver(String id, ToolEvent<T> event) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public void onBeforeInitJob(ToolEvent<T> event) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void onBeforeMapReduceSubmit(ToolEvent<T> event) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void onAfterMapReduceFinish(int retVal, ToolEvent<T> event) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void onBeforeExit(ToolEvent<T> event) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void onJobException(ToolEvent<T> event, Exception e) throws Exception {
		// TODO Auto-generated method stub

	}

}
