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


import java.util.Enumeration;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.UnrecognizedOptionException;

/**
 * An extension to the base {@link org.apache.commons.cli.PosixParser} that
 * allows 'required' arguments to be passed in a properties file
 * as well as the command line. By default, the properties file props
 * aren't removed from the required list.
 *
 */
public class PosixParserRequiredProps extends PosixParser {

	/*
	 * {@inheritDoc}
	 * 
	 * Before calling super, this method will validate the properties
	 * looking for undefined arguments.
	 */
	@Override
	public CommandLine parse(Options options, String[] arguments,
			Properties properties, boolean stopAtNonOption)
					throws ParseException {

		// This check isn't performed in the base library.
		validateProperties(options, properties);

		return super.parse(options, arguments, properties, stopAtNonOption);
	}

	/**
	 * Verifies that the properties don't contain undefined options which will
	 * cause the base parser to blowup.
	 *
	 * @param options						the options config
	 * @param properties					overriding properties
	 * @throws UnrecognizedOptionException	if a property exists that isn't
	 * 										configured in the options.
	 */
	protected void validateProperties(Options options, Properties properties)
			throws UnrecognizedOptionException {
		if (properties != null) {
			for (Entry<Object, Object> e : properties.entrySet()) {
				String arg = (String) e.getKey();
				boolean hasOption = options.hasOption(arg);
				if (!hasOption) {
					throw new UnrecognizedOptionException(
							"Unrecognized option: " + arg, arg);
				}
			}
		}
	}

	@Override
	protected void processProperties(Properties properties) {
		super.processProperties(properties);
		if (properties == null) {
			return;
		}

		for (Enumeration<?> e = properties.propertyNames(); e.hasMoreElements();) {
			String option = e.nextElement().toString();
			if (cmd.hasOption(option)) {
				Option opt = getOptions().getOption(option);
				this.getRequiredOptions().remove(opt.getOpt());
			}
		}
	}
}
