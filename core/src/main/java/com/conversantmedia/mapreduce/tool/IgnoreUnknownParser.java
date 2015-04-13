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


import java.util.ListIterator;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * Parser override to ignore unknown options. This
 * enables us to parse config file option
 *
 */
public class IgnoreUnknownParser extends PosixParser {

	@Override
	@SuppressWarnings("rawtypes")
	protected void processOption(String arg, ListIterator iter)
			throws ParseException {
		boolean hasOption = getOptions().hasOption(arg);
		if (hasOption) {
			super.processOption(arg, iter);
		}
	}
}
