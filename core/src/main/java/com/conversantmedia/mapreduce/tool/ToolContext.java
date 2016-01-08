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


import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base context for a MapReduce Application/Tool
 *
 */
public class ToolContext {

	public static final String CONF_KEY_NAMESPACE = "cnvr.mapreduce";
	public static final String CONF_KEY_PREFIX = CONF_KEY_NAMESPACE + ".";
	// Our job context uuid
	public static final String CONF_KEY_JOB_UUID = CONF_KEY_PREFIX + "job.uuid";
	
	// Source path argument where we'll find the inputs
	public static final String OPTION_INPUT_PATH = "input";

	// Destination path argument where the processed output
	// should be placed.
	public static final String OPTION_OUTPUT_PATH = "output";

	// Location to "archive" the input file(s) to. This is where
	// the file goes following processing, which may or may not
	// actually be considered an "archival" location - it may shuffle
	// off for futher handling. However, 'archive' typically applies.
	public static final String OPTION_ARCHIVE_PATH = "archive";

	// Dumps the Hadoop configuration key/value pairs to stdout
	public static final String OPTION_DUMP_CONFIG = "dump";

	// Setup the job but don't execute
	public static final String OPTION_DRY_RUN = "dryRun";

	// An optional configuration properties file for specifying command-line options
	// via an external file.
	public static final String OPTION_OPTIONS_FILE = "options";

	// Verbosity settings
	private static final String OPTION_VERBOSE = "verbose";
	private static final String OPTION_TERSE = "terse";
	private static final String OPTION_SILENT = "silent";

	//OpenTSDB configurations
	public static final String OPTION_PUBLISH_TSDB_METRIC = "publish-tsdb-metric";
	public static final String OPTION_OPENTSDB_METRIC_NAME = "tsdb-metric-name";

	private ToolContextListener contextListener;

	// A unique id for our context
	private final String id = UUID.randomUUID().toString();

	private Path[] input;

	private Path output;

	private Path archive;

	private boolean dumpConfig;

	private boolean dryRun;

	private Job job;

	private int returnCode;

	private Long mapReduceStartTime;

	private Long mapReduceEndTime;

	private boolean publishTsdb;

	private String tsdbMetricName;

	private Class<?> driverClass;

	/**
	 * Re-parse our options using the properties from the specified conf file.
	 * @param properties		the command line override properties
	 * @param args				command line arguments
	 * @return					any left-over command line arguments
	 * 			that haven't been parsed. This method sets up and parses
	 * 			only the option file argument, so the return would be everything
	 * 			from the command line args except the --file if provided.
	 * @throws ParseException	if unable to parse the command line options
	 * @throws IOException		if unable to read the file
	 */
	@SuppressWarnings("static-access")
	protected String[] getConfigOverrides(Properties properties, String[] args) throws ParseException, IOException {
		// Parse with our config option only...
		Options options = new Options();
		options.addOption(
				OptionBuilder.withLongOpt(OPTION_OPTIONS_FILE)
				.withDescription("Configuration file of key/value pairs with option overrides.")
				.withArgName("file")
				.hasArg()
				.create());

		Parser parser = new IgnoreUnknownParser();
		CommandLine line = parser.parse(options, args, true);
		if (line.hasOption(OPTION_OPTIONS_FILE)) {
			File confFile = new File(line.getOptionValue(OPTION_OPTIONS_FILE));
			if (!confFile.exists()) {
				throw new IllegalArgumentException("Configuration file not found.");
			}
			Reader reader = null;
			try {
				reader = new FileReader(confFile);
				properties.load(reader);
			}
			finally {
				IOUtils.closeQuietly(reader);
			}
		}

		return line.getArgs();
	}

	@SuppressWarnings("static-access")
	protected final Options initOptions() {
		Options options = new Options();

		// Help
		options.addOption(
				OptionBuilder.withLongOpt("help")
				.withDescription("Print this help message")
				.isRequired(false)
				.create('?'));

		// Required
		options.addOption(
				OptionBuilder.withLongOpt(OPTION_INPUT_PATH)
				.withDescription("Input path")
				.withArgName("path")
				.hasArg()
				.isRequired(true)
				.create('i'));

		// Optional arguments
		options.addOption(
				OptionBuilder.withLongOpt(OPTION_ARCHIVE_PATH)
				.withDescription("Path to move the input file(s) to following processing.")
				.withArgName("path")
				.hasArg()
				.isRequired(false)
				.create('a'));

		options.addOption(
				OptionBuilder.withLongOpt(OPTION_OUTPUT_PATH)
				.withDescription("Output path for this job's output")
				.withArgName("path")
				.hasArg()
				.isRequired(false)
				.create('o'));

		options.addOption(
				OptionBuilder.withLongOpt(OPTION_DUMP_CONFIG)
				.withDescription("Dump ALL configuration key/value pairs to STDOUT")
				.isRequired(false)
				.create());

		options.addOption(
				OptionBuilder.withLongOpt(OPTION_DRY_RUN)
				.withDescription("Performs all setup tasks without executing MR job")
				.isRequired(false)
				.create());

		options.addOption(
				OptionBuilder.withLongOpt(OPTION_SILENT)
				.withDescription("Only output result.")
				.isRequired(false)
				.create("b")
				);

		options.addOption(
				OptionBuilder.withLongOpt(OPTION_TERSE)
				.withDescription("Output short messages.")
				.isRequired(false)
				.create("v")
				);

		options.addOption(
				OptionBuilder.withLongOpt(OPTION_VERBOSE)
				.withDescription("Output verbose messages.")
				.isRequired(false)
				.create("vv")
				);

		options.addOption(
				OptionBuilder.withLongOpt(OPTION_PUBLISH_TSDB_METRIC)
				.withArgName("true|false")
				.withDescription("Do not publish metrics and throughtputs to Open TSDB.")
				.hasArg()
				.isRequired(false)
				.create());

		options.addOption(
				OptionBuilder.withLongOpt(OPTION_OPENTSDB_METRIC_NAME)
				.withDescription("OpenTSDB metric name.")
				.withArgName("name")
				.hasArg()
				.isRequired(false)
				.create()
				);

		// For sub-classes
		initExtraOptions(options);

		return options;
	}

	/**
	 * Parse the arguments.
	 * 
	 * @param args				the command line arguments
	 * @throws Exception		if the process fails
	 * @throws ParseException	if the command line parsing fails
	 */
	protected void parseFromArgs(String[] args) throws Exception {
		Options options = initOptions();

		// Notify our listener if there is one...
		if (this.contextListener != null) {
			this.contextListener.afterInitOptions(options);
		}

		try {
			args = handleHelp(args, options);

			Properties props = new Properties();
			args = getConfigOverrides(props, args);

			// Now parse the rest of 'em
			Parser parser = new PosixParserRequiredProps();
			CommandLine line = null;
			line = parser.parse(options, args, props, false);

			// Populate our context values from the parsed command line
			populateContext(line);

			// Notify our listener if there is one...
			if (this.contextListener != null) {
				this.contextListener.afterParseCommandLine(line);
			}
		}
		catch (ParseException e) {
			showHelpAndExit(options, 1, e.getMessage());
		}
	}

	protected void showHelpAndExit(Options options, int exitCode) {
		showHelpAndExit(options, exitCode, null);
	}
	
	protected void showHelpAndExit(Options options, int exitCode, String message) {
		if (StringUtils.isNotBlank(message)) { Console.all(message); }
		new HelpFormatter().printHelp(getCommandLineSyntax(), options);
		System.exit(exitCode);
	}

	private String[] handleHelp(String[] args, Options fullOptions) {
		Options options = new Options();
		options.addOption(fullOptions.getOption("help"));
		Parser p = new PosixParser();
		try {
			CommandLine line = p.parse(options, args, true);
			if (line.hasOption('?')) {
				showHelpAndExit(fullOptions, 0);
				new HelpFormatter().printHelp(getCommandLineSyntax(), fullOptions);
				System.exit(0);
			}
			return line.getArgs();
		} catch (ParseException e) {
			// ignore
		}
		return args;
	}

	/**
	 * Populates the context from the {@link CommandLine}
	 * 
	 * @param line	the command line
	 */
	private void populateContext(CommandLine line) throws ParseException {
		String[] inputPaths = StringUtils.split(line.getOptionValue(OPTION_INPUT_PATH), ",");
		Path[] input = new Path[inputPaths.length];
		for (int i = 0; i < input.length; i++) {
			input[i] = new Path(inputPaths[i]);
		}
		setInput(input);

		if (line.hasOption(OPTION_OUTPUT_PATH)) {
			Path output = new Path(line.getOptionValue(OPTION_OUTPUT_PATH, ""));
			setOutput(output);
		}

		if (line.hasOption(OPTION_ARCHIVE_PATH)) {
			Path archive = new Path(line.getOptionValue(OPTION_ARCHIVE_PATH));
			setArchive(archive);
		}

		if (line.hasOption(OPTION_ARCHIVE_PATH)) {
			Path archive = new Path(line.getOptionValue(OPTION_ARCHIVE_PATH));
			setArchive(archive);
		}

		this.dumpConfig = line.hasOption(OPTION_DUMP_CONFIG);
		this.dryRun = line.hasOption(OPTION_DRY_RUN);

		// Setup the console
		Console.setMode(Console.TERSE);
		if (line.hasOption(OPTION_VERBOSE)) {
			Console.setMode(Console.VERBOSE);
		}
		else if (line.hasOption(OPTION_SILENT)) {
			Console.setMode(Console.SILENT);
		}

		// OpenTSDB
		setPublishTsdbMetric(
				line.hasOption(OPTION_PUBLISH_TSDB_METRIC) // don't publish by default
				&& line.getOptionValue(OPTION_PUBLISH_TSDB_METRIC).equalsIgnoreCase("true"));
		if (line.hasOption(OPTION_OPENTSDB_METRIC_NAME)) {
			setTsdbMetricName(line.getOptionValue(OPTION_OPENTSDB_METRIC_NAME));
		}

		populateExtendedContext(line);
	}

	public String getCommandLineSyntax() {
		return "hadoop jar <jar-name> [hadoop-options] <tool-options>";
	}

	/**
	 * Copy any of our command-line options that are needed by the configuration
	 * Default implementation does nothing.
	 * 
	 * @param configuration	the job configuration
	 */
	public void copyPropertiesToConf(Configuration configuration) {
		// do-nothing default implementation
	}

	/**
	 * Extension point for subclasses to add their own extended options.
	 * Default implementation does nothing.
	 * 
	 * @param options	command line options
	 */
	protected void initExtraOptions(Options options) {
		// do-nothing default implementation.
	}

	/**
	 * Extension point for sub-classes to do context setup without
	 * having to call super. Allows us to finalize out primary method.
	 * Default implementation does nothing.
	 * 
	 * @param line				command line
	 * @throws ParseException	if the parsing of the options fails
	 */
	protected void populateExtendedContext(CommandLine line) throws ParseException {
		// do-nothing default implementation
	}

	public String getId() {
		return this.id;
	}

	public Path[] getInput() {
		return input;
	}

	public void setInput(Path[] input) {
		this.input = input;
	}

	public Path getOutput() {
		return output;
	}

	public void setOutput(Path output) {
		this.output = output;
	}

	public Path getArchive() {
		return archive;
	}

	public void setArchive(Path archive) {
		this.archive = archive;
	}

	public boolean isDumpConfig() {
		return dumpConfig;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}

	public Long getMapReduceStartTime() {
		return mapReduceStartTime;
	}

	public void setMapReduceStartTime(Long mapReduceStartTime) {
		this.mapReduceStartTime = mapReduceStartTime;
	}

	public Long getMapReduceEndTime() {
		return mapReduceEndTime;
	}

	public void setMapReduceEndTime(Long mapReduceEndTime) {
		this.mapReduceEndTime = mapReduceEndTime;
	}

	public int getReturnCode() {
		return returnCode;
	}

	public void setReturnCode(int returnCode) {
		this.returnCode = returnCode;
	}

	public boolean isPublishTsdbMetric() {
		return this.publishTsdb;
	}

	public void setPublishTsdbMetric(boolean enable) {
		this.publishTsdb = enable;
	}

	public String getTsdbMetricName() {
		return this.tsdbMetricName;
	}

	public void setTsdbMetricName(String tsdbMetricName) {
		this.tsdbMetricName = tsdbMetricName;
	}

	public Class<?> getDriverClass() {
		return driverClass;
	}

	void setDriverClass(Class<?> driverClass) {
		this.driverClass = driverClass;
	}

	public void putTsdbTags(Map<String, String> tags) {
		// do-nothing default impl.
	}

	@Override
	public String toString() {
		int lc = getLeftColumnWidth(), rc = getRightColumnWidth();
		StringBuffer buf = new StringBuffer();
		buf.append("\n").append(getClass().getSimpleName()).append(":\n");
		buf.append(StringUtils.repeat("=", lc + rc));
		buf.append("\n");
		argsToString(buf, lc, rc);
		buf.append(StringUtils.repeat("=", lc + rc));
		buf.append("\n");
		return buf.toString();
	}

	protected void toRowString(StringBuffer sb, String left, String right,
			int lc, int rc) {
		sb.append(StringUtils.rightPad(left + ":", lc, '.'));
		sb.append(StringUtils.leftPad("[" + right + "]", rc, '.'));
		sb.append("\n");
	}

	/**
	 *
	 * @param sb buffer to write to
	 * @param lc left hand column width
	 * @param rc right hand column width
	 */
	protected void argsToString(StringBuffer sb, int lc, int rc) {
		toRowString(sb, "Input", StringUtils.join(getInput(), ","), lc, rc);

		if (this.getOutput() != null) {
			toRowString(sb, "Output", getOutput().toString(), lc, rc);
		}

		if (this.getArchive() != null) {
			toRowString(sb, "Archive", getArchive().toString(), lc, rc);
		}
		toRowString(sb, "Publish to TSDB?", "" + this.isPublishTsdbMetric(), lc, rc);
		toRowString(sb, "Dump config?", "" + isDumpConfig(), lc, rc);
		toRowString(sb, "Dry run?", "" + isDryRun(), lc, rc);
	}

	protected int getLeftColumnWidth() {
		return 28;
	}

	protected int getRightColumnWidth() {
		return 72;
	}

	protected void setContextListener(ToolContextListener contextListener) {
		this.contextListener = contextListener;
	}

	protected Logger logger() {
		return LoggerFactory.getLogger(this.getClass());
	}

	protected interface ToolContextListener {
		void afterInitOptions(Options options) throws Exception;
		void afterParseCommandLine(CommandLine commandLine) throws Exception;
	}

}
