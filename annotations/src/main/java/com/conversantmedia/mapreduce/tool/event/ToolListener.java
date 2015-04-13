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
 * An event listener that may register to be informed of events
 * on the driver.
 *
 * @param <T> the driver context bean type
 */
public interface ToolListener<T> {

	public static enum Event {
		AFTER_INIT_CLI_OPTIONS, AFTER_PARSE_CLI, BEFORE_INIT_DRIVER, BEFORE_INIT_JOB, BEFORE_MR_SUBMIT, AFTER_MR_FINISH, BEFORE_EXIT, EXCEPTION
	}

	/**
	 * Called immediately following CLI initialization.
	 * @param event			the event that occurred			the event that occurred
	 * @param options		the driver's parsed command line options
	 * @throws Exception	if an exception occurs during handling
	 */
	void onAfterInitCliOptions(ToolEvent<T> event, Options options) throws Exception;

	/**
	 * Called immediately following CLI initialization.
	 * @param event			the event that occurred
	 * @param line			the command line arguments
	 * @throws Exception	if an exception occurs during handling
	 */
	void onAfterParseCli(ToolEvent<T> event, CommandLine line) throws Exception;

	/**
	 * Called just prior to BaseTool.init being called.
	 * @param id			the uuid generated for this driver instance
	 * @param event			the event that occurred
	 * @throws Exception	if an exception occurs during handling
	 */
	void onBeforeInitDriver(String id, ToolEvent<T> event) throws Exception;

	/**
	 * Called just prior to BaseTool.initJob being called.
	 * @param event			the event that occurred
	 * @throws Exception	if an exception occurs during handling
	 */
	void onBeforeInitJob(ToolEvent<T> event) throws Exception;

	/**
	 * Called just prior to submitting MR job.
	 * @param event			the event that occurred
	 * @throws Exception	if an exception occurs during handling
	 */
	void onBeforeMapReduceSubmit(ToolEvent<T> event) throws Exception;

	/**
	 * Called following a MapReduce job run.
	 * @param retVal		the application exit code
	 * @param event			the event that occurred
	 * @throws Exception	if an exception occurs during handling
	 */
	void onAfterMapReduceFinish(int retVal, ToolEvent<T> event) throws Exception;

	/**
	 * Called just prior to exit but after the default 'cleanup'
	 * @param 				event	the event that occurred
	 * @throws Exception	if an exception occurs during handling
	 */
	void onBeforeExit(ToolEvent<T> event) throws Exception;

	/**
	 * Called if the job throws an exception.
	 * @param 				event	the event that occurred
	 * @param e				the exception encountered
	 * @throws Exception	if an exception occurs during handling
	 */
	void onJobException(ToolEvent<T> event, Exception e) throws Exception;
}
