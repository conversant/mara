package com.conversantmedia.mapreduce.tool;

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


import static com.conversantmedia.mapreduce.tool.Console.TERSE;
import static com.conversantmedia.mapreduce.tool.Console.VERBOSE;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.conversantmedia.mapreduce.tool.ToolContext.ToolContextListener;
import com.conversantmedia.mapreduce.tool.event.DefaultToolEvent;
import com.conversantmedia.mapreduce.tool.event.ToolEvent;
import com.conversantmedia.mapreduce.tool.event.ToolListener;

/**
 * Base tool for use by MapReduce jobs.
 *
 * Subclasses must minimally implement <tt>initJob</tt> and provide their own
 * main method similar to the following:
 * <pre>
 * 	public static void main(String[] args) {
 *		int res;
 *		try {
 *			res = ToolRunner.run(new Configuration(), new DeviceIdLoaderTool(), args);
 *		}
 *		catch (Exception e) {
 *			e.printStackTrace();
 *			res = 1;
 *		}
 *		System.exit(res);
 *	}
 * </pre>
 *
 * Subclasses may optionally override:
 * <ul>
 * <li><tt>init</tt> - called just prior to initJob
 * <li><tt>cleanUp</tt> - called following execution and optional archival step
 * <li><tt>newContext</tt> - enables overriding context type
 * </ul>
 *
 *  @see ToolContext for base command line options.
 *
 *
 */
public abstract class BaseTool<T extends ToolContext>
	extends Configured implements Tool, ToolListener<T> {

	private final List<ToolListener<T>> listeners = new ArrayList<>();

	private Type contextType;

	public BaseTool() {
		// Grab a reference to the tool context type for use
		// by the driver container for annotated tools.
		Type superclass = getClass().getGenericSuperclass();
		if (! (superclass instanceof Class)) {
			this.contextType = ((ParameterizedType) superclass).getActualTypeArguments()[0];
		}
	}

	/**
	 * Subclasses must return an appropriate context.
	 * @return					the context type
	 * @throws	ToolException 	if the creation fails
	 */
	protected abstract T newContext() throws ToolException;

	/**
	 * Initializes/configures our job.
	 * @param context		the driver context bean
	 * @return				the job
	 * @throws Exception	if unable to initialize for any reason
	 */
	public abstract Job initJob(T context) throws Exception;

	@Override
	public int run(String[] args) throws Exception {
		final T context = newContext();
		context.setContextListener(new ToolContextListener() {
			@Override
			public void afterInitOptions(Options options) throws Exception {
				notifyListeners(Event.AFTER_INIT_CLI_OPTIONS, context, options);
			}

			@Override
			public void afterParseCommandLine(CommandLine commandLine)
					throws Exception {
				notifyListeners(Event.AFTER_PARSE_CLI, context, commandLine);
			}
		});

		context.setDriverClass(this.getClass());

		try {
			// Register ourselves as a listener
			this.addListener(this);

			context.parseFromArgs(args);

			// Useful info
			logger().info(context.toString());

			// Perform any specific initialization tasks
			notifyListeners(Event.BEFORE_INIT_DRIVER, context, null);
			initInternal(context);

			// Notify any listeners before initializing job
			notifyListeners(Event.BEFORE_INIT_JOB, context, null);

			// Initialize our job
			Job job = initJob(context);
			// We can override (the runjob script does) which jar to use instead of using running driver class
			if (StringUtils.isBlank(job.getConfiguration().get("mapred.jar"))) {
				logger().info("Setting job jar by class [" + this.getClass() + "]");
				job.setJarByClass(this.getClass());
			}
			context.setJob(job);

			// Post-initialization routines
			jobPostInit(context);

			if (context.isDumpConfig()) {
				Console.out(context.toString());
				dumpConfig(job.getConfiguration());
			}

			if (context.isDryRun()) {
				Console.out("Dry run only. Job will not be executed.");
				return 0;
			}

			// Launches the job
			launchJob(context, job);

			// Now move our input to archive
			if (context.getReturnCode() == 0  && context.getArchive() != null) {
				archiveInputs(context);
			}

			// Clean up our job
			cleanUp(context);

			notifyListeners(Event.BEFORE_EXIT, context, null);
		}
		catch (ParseException pe) {
			// Output a more "friendly" message
			context.showHelpAndExit(context.initOptions(), 1, pe.getMessage());
		}
		catch (Exception e) {
			logger().error("Problem running tool: " + e.getMessage(), e);
			notifyListeners(Event.EXCEPTION, context, e);
		}

		return context.getReturnCode();
	}

	/**
	 * Post-job initialization
	 * @param context			the driver context bean
	 * @throws ToolException	if the job 
	 */
	protected void jobPostInit(T context) throws ToolException {
		// Handle @Distribute annotation
		distributeResources(context.getJob(), this, context);
	}

	/**
	 * Perform resource distribution on beans annotated
	 * with @Distribute.
	 * @param job			    the job configuration
	 * @param beans			    resources to distribute. Any resource
	 * 			must implement java.io.Serializable.
	 * @throws ToolException	If any of the objects cannot be
	 * 			serialized onto the distributed cache. 
	 */
	void distributeResources(Job job, Object...beans) throws ToolException {
		DistributedResourceManager distManager = new DistributedResourceManager(job);
		try {
			for (Object bean : beans) {
				distManager.configureBeanDistributedResources(bean);
			}
		} catch (IllegalAccessException | InvocationTargetException
				| NoSuchMethodException | IllegalArgumentException
				| IOException e) {
			throw new ToolException(e);
		}
	}

	protected void launchJob(T context, Job job) throws Exception {
		// Wait for completion and return
		int returnCode = 1;

		// Inform listeners we're about to begin
		notifyListeners(Event.BEFORE_MR_SUBMIT, context, null);

		// Note the start time
		context.setMapReduceStartTime(System.currentTimeMillis());

		returnCode = job.waitForCompletion(true)? 0 : 1;

		// Set the result on our context
		context.setReturnCode(returnCode);

		// Set the end time.
		context.setMapReduceEndTime(System.currentTimeMillis());

		// All done!
		notifyListeners(Event.AFTER_MR_FINISH, context, null);

		logger().debug("MapReduce return code: " + context.getReturnCode());
	}

	protected List<FileStatus> getInputFiles(Path input) throws IOException {
		FileSystem fs = FileSystem.get(getConf());
		List<FileStatus> status = new ArrayList<>();
		if (fs.exists(input)) {
			FileStatus inputStatus = fs.getFileStatus(input);
			if (inputStatus.isDirectory()) {
				// Move all files under this directory
				status = Arrays.asList(fs.listStatus(input));
			}
			else {
				status.add(inputStatus);
			}
		}
		// Must be a glob path
		else {
			FileStatus[] statusAry = fs.globStatus(input);
			status.addAll(Arrays.asList(statusAry));
		}
		return status;
	}

	/**
	 * Moves our inputs into the 'archive' path for
	 * long term storage, or perhaps further processing.
	 * @param context		the job's driver context bean
	 * @throws IOException	if the inputs cannot be moved to
	 * 			the archive path.
	 */
	protected void archiveInputs(T context) throws IOException {
		FileSystem fs = FileSystem.get(getConf());
		fs.mkdirs(context.getArchive());

		for (Path input : context.getInput()) {
			List<FileStatus> status = getInputFiles(input);
			for (FileStatus file : status) {
				Path dest = new Path(context.getArchive(), file.getPath().getName());
				fs.rename(file.getPath(), dest);
				logger().debug("Moved [" + input + "] to [" + dest + "]");
			}
		}
	}

	/**
	 * Initialize the tool. Called immediately preceding 'initJob'.
	 * @param context		the job's driver context bean
	 * @throws Exception	if initialization fails
	 */
	protected final void initInternal(T context) throws Exception {
		Configuration conf = this.getConf();

		// Allow the context to add some things...
		context.copyPropertiesToConf(conf);

		// Here's the one subclasses can override
		init(context);
	}

	protected void init(T context) throws Exception {
		// do nothing base implementation.
	}

	/**
	 * Clean up method is called immediately following 'archiveInputs'.
	 * Base implementation does nothing so that subclasses don't have
	 * to explicitly implement.
	 * 
	 * @param context		the job's driver context bean
	 * @throws Exception	if the operation fails
	 */
	protected void cleanUp(T context)
			throws Exception {
		// do nothing base implementation.
	}

	// ToolListener interface methods
	@Override
	public void onAfterInitCliOptions(ToolEvent<T> event, Options options) throws Exception {
		// do-nothing
	}

	@Override
	public void onAfterParseCli(ToolEvent<T> event, CommandLine line) throws Exception {
		// do-nothing
	}

	@Override
	public void onBeforeInitDriver(String id, ToolEvent<T> event) throws Exception {
		// do-nothing
	}

	@Override
	public void onBeforeInitJob(ToolEvent<T> event) throws Exception {
		// do-nothing
	}

	@Override
	public void onBeforeMapReduceSubmit(ToolEvent<T> event) throws Exception {
	}

	@Override
	public void onAfterMapReduceFinish(int retVal, ToolEvent<T> event) throws Exception {
	}

	@Override
	public void onBeforeExit(ToolEvent<T> event) throws Exception{
		// Output the context id so that it may be piped
		// into other processes if required.
		Console.out(TERSE | VERBOSE, event.getContext().getId());
	}

	@Override
	public void onJobException(ToolEvent<T> event, Exception e) throws Exception {
		event.getContext().setReturnCode(1);
		logger().error(e.getMessage(), e);
		Console.error("Exception runing job: " + e.getMessage());
	}

	/**
	 * Dumps the configuration to stdout.
	 * @param config	the job configuration
	 */
	protected void dumpConfig(Configuration config) {
		Console.out("Dumping Configuration:");
		for (Entry<String, String> entry : config) {
			Console.out(entry.getKey() + " ==> " + entry.getValue());
		}
	}

	/**
	 * Register an event listener.
	 * @param listener	a listener for driver events
	 */
	public void addListener(ToolListener<T> listener) {
		this.listeners.add(listener);
	}

	public void notifyListeners(ToolListener.Event eventType, T context, Object subject) throws Exception {
		ToolEvent<T> event = new DefaultToolEvent<>(context);
		for (ToolListener<T> listener : listeners) {
			switch (eventType) {
			case AFTER_INIT_CLI_OPTIONS:
				listener.onAfterInitCliOptions(event, (Options)subject);
				break;
			case AFTER_PARSE_CLI:
				listener.onAfterParseCli(event, (CommandLine)subject);
				break;
			case BEFORE_INIT_DRIVER:
				listener.onBeforeInitDriver(event.getContext().getId(), event);
				break;
			case BEFORE_INIT_JOB:
				listener.onBeforeInitJob(event);
				break;
			case BEFORE_MR_SUBMIT:
				listener.onBeforeMapReduceSubmit(event);
				break;
			case AFTER_MR_FINISH:
				listener.onAfterMapReduceFinish(event.getContext().getReturnCode(), event);
				break;
			case BEFORE_EXIT:
				listener.onBeforeExit(event);
				break;
			case EXCEPTION:
				listener.onJobException(event, (Exception)subject);
				break;
			}
		}
	}

	public Type getContextType() {
		return contextType;
	}

	protected final Logger logger() {
		return LoggerFactory.getLogger(this.getClass());
	}

}
