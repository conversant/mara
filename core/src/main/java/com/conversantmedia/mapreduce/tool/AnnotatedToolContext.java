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


import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.SimpleTypeConverter;

import com.conversantmedia.mapreduce.tool.annotation.Option;
/**
 *
 *
 */
public class AnnotatedToolContext extends ToolContext {

	private final Object bean;

	private Map<String, Field> fieldsMap;
	private Map<String, Field> addedOptionsMap;
	private Map<String, String> defaultValuesMap;

	public AnnotatedToolContext(Object object) {
		this.bean = object;
	}

	public Object getAnnotatedBean() {
		return this.bean;
	}

	@Override
	protected void initExtraOptions(Options options) {
		// Find all the fields with @Option annotations...
		fieldsMap = new HashMap<>();
		addedOptionsMap = new HashMap<>();
		defaultValuesMap = new HashMap<>();

		Class<?> clazz = this.bean.getClass();
		while (clazz != Object.class) {
			for (Field field : clazz.getDeclaredFields()) {
			if (field.isAnnotationPresent(Option.class)) {
					Option option = field.getAnnotation(Option.class);
					// Ensure we don't add the same option more than 1x
					// For example, if our annotated bean includes an 'input',
					// we don't want to add a second version
					String optName = getValue(option.name(), field.getName());
					org.apache.commons.cli.Option opt;
					if (options.hasOption(optName)) {
						opt = options.getOption(optName);
						updateOption(option, opt);
					}
					else {
						opt = initOption(options, option, optName);
						options.addOption(opt);
						addedOptionsMap.put(opt.getLongOpt(), field);
					}
					fieldsMap.put(opt.getLongOpt(), field);
					defaultValuesMap.put(opt.getLongOpt(), option.defaultValue());
				}
			}
			clazz = clazz.getSuperclass();
		}
	}

	/**
	 * Override the
	 * @param source
	 * @param base
	 */
	private void updateOption(Option source,
			org.apache.commons.cli.Option base) {
		if (StringUtils.isNotBlank(source.description())) {
			base.setDescription(source.description());
		}
		if (StringUtils.isNotBlank(source.argName())) {
			base.setArgName(source.argName());
		}
		if (source.argCount() > 0) {
			base.setArgs(source.argCount());
		}

		if (!base.isRequired()) {
			base.setRequired(source.required());
		}
	}

	@SuppressWarnings("static-access")
	private org.apache.commons.cli.Option initOption(Options options, Option anno, String optName) {
		OptionBuilder.withLongOpt(optName)
			.withArgName(getValue(anno.argName()))
			.withDescription(getValue(anno.description()))
			.isRequired(anno.required());

		if (anno.argCount() > 0) {
			OptionBuilder.hasArgs(anno.argCount());
		}

		return OptionBuilder.create();
	}

	private String getValue(String value, String defaultValue) {
		return StringUtils.isNotBlank(value)? value : defaultValue;
	}

	private String getValue(String value) {
		return getValue(value, null);
	}

	@Override
	protected void populateExtendedContext(CommandLine line) throws ParseException {
		// Populate the underlying annotated context object
		// with the values from the parsed command line arguments.
		SimpleTypeConverter converter = new SimpleTypeConverter();
		
		for (Entry<String, Field> e : this.fieldsMap.entrySet()) {
			String optName = e.getKey();
			Field field = e.getValue();
			String defaultValue = defaultValuesMap.get(optName);
			field.setAccessible(true);
			try {
				if (line.hasOption(optName)) {
					Object value = line.getOptionValue(optName);
					if (value == null) {
						value = Boolean.TRUE;
					}
					value = converter.convertIfNecessary(value, field.getType());
					field.set(this.bean, value);
				}
				else if (StringUtils.isNotBlank(defaultValue)){
					Object value = converter.convertIfNecessary(defaultValue, field.getType());
					field.set(this.bean, value);
				}
			} catch (IllegalArgumentException | IllegalAccessException e1) {
				throw new ParseException(e1.getMessage());
			}
		}
	}

	@Override
	public void copyPropertiesToConf(Configuration conf) {
		conf.set(CONF_KEY_JOB_UUID, this.getId());
	}

	@Override
	public String toString() {
		int lc = getLeftColumnWidth(), rc = getRightColumnWidth();
		StringBuffer buf = new StringBuffer();
		// Overriding this method solely to sub out the name in the output
		buf.append("\n").append(this.bean.getClass().getSimpleName()).append(":\n");
		buf.append(StringUtils.repeat("=", lc + rc));
		buf.append("\n");
		argsToString(buf, lc, rc);
		buf.append(StringUtils.repeat("=", lc + rc));
		buf.append("\n");
		return buf.toString();
	}

	@Override
	protected void argsToString(StringBuffer sb, int lc, int rc) {
		super.argsToString(sb, lc, rc);
		for (Entry<String, Field> e : this.addedOptionsMap.entrySet()) {
			String optName = e.getKey();
			Field field = e.getValue();
				field.setAccessible(true);
				try {
					Object value = field.get(this.bean);
					if (value != null) {
						toRowString(sb, optName, String.valueOf(value), lc, rc);
					}
				} catch (IllegalArgumentException | IllegalAccessException e1) {
					e1.printStackTrace();
				}
		}
	}
}
