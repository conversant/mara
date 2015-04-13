package com.conversantmedia.mapreduce.tool.annotation.handler;

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


import com.conversantmedia.mapreduce.tool.AnnotatedTool;
import com.conversantmedia.mapreduce.tool.ExpressionEvaluator;
import com.conversantmedia.mapreduce.tool.ToolException;

public abstract class AnnotationHandlerBase implements MaraAnnotationHandler {

	private AnnotatedTool tool;

	@Override
	public void initialize(AnnotatedTool tool) {
		this.tool = tool;
	}

	protected AnnotatedTool getAnnotatedTool() {
		return this.tool;
	}

	protected Object evaluateExpression(String expr) throws ToolException {
		// If this isn't an expression, will simply return itself
		return ExpressionEvaluator.instance()
				.evaluate(getAnnotatedTool().getToolBean(), this.getAnnotatedTool().getContext(), expr);
	}
}
