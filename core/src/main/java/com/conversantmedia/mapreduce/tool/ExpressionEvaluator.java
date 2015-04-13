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


import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ognl.DefaultMemberAccess;
import ognl.Ognl;
import ognl.OgnlContext;
import ognl.OgnlException;

import org.apache.commons.lang3.StringUtils;

/**
 *
 *
 */
public class ExpressionEvaluator {

	private static final String TOOL_CONTEXT_KEY = "_context";

	private static ExpressionEvaluator instance;

	private Pattern pattern = null;

	private ExpressionEvaluator() {
		this.pattern = Pattern.compile("(.*)\\$\\{(.+)\\}(.*)",Pattern.DOTALL);
	}

	public static ExpressionEvaluator instance() {
		if (instance == null) {
			instance = new ExpressionEvaluator();
		}
		return instance;
	}

	/**
	 * Convenience method for evaluating the provided expression using
	 * elements of the annotated tool in the root/context.
	 * 
	 * @param tool				the tool bean
	 * @param expr				the OGNL expression to evaluate
	 * @return					the result of the evaluation
	 * @throws ToolException	if something goes wrong
	 */
	public Object evaluate(AnnotatedTool tool, String expr) throws ToolException {
		return evaluate(tool.getToolBean(), tool.getContext(), expr);
	}

	/**
	 * Convenience method for performing expression evaluation on the
	 * tool with the context in the context map.
	 * 
	 * @param tool				the tool bean
	 * @param context			the tool context
	 * @param expr				the OGNL expression to evaluate
	 * @return					the result of the evaluation
	 * @throws ToolException	if something goes wrong
	 */
	public Object evaluate(Object tool, AnnotatedToolContext context, String expr) throws ToolException {
		Map<String, Object> ognlContext = new HashMap<String, Object>();
		ognlContext.put("context", context.getAnnotatedBean());
		return evaluate(tool, ognlContext, expr);
	}

	/**
	 *
	 * @param rootObj			root object for the OGNL context
	 * @param context			set of properties fed into evaluation
	 * @param expr				the expression to evaluate. Expects an OGNL expression.
	 * @return					the result of the expression's evaluation
	 * @throws ToolException	if something goes wrong
	 */
	@SuppressWarnings("rawtypes")
	public Object evaluate(Object rootObj, Map<String, Object> context, String expr) throws ToolException {

		Object value = expr;
		Object expressionValue = null;

		Map ognlContext = buildOgnlContext(rootObj, context);

		try {
			Matcher matcher = pattern.matcher(expr);
			while (matcher.matches()) {
				expr = matcher.group(2); // remove the enclosing ${ and }

				// If it doesn't start with 'context.' or 'this.' then prepend the
				// expression with 'this.'
				if (!(StringUtils.startsWith(expr, OgnlContext.THIS_CONTEXT_KEY + ".")
						|| StringUtils.startsWith(expr, "context."))) {
					expr = "this." + expr;
				}
				else  {
					expr = StringUtils.replace(expr, "context", TOOL_CONTEXT_KEY);
				}

				expressionValue = Ognl.getValue(expr, ognlContext);
				if (StringUtils.isNotBlank(matcher.group(1)) || StringUtils.isNotBlank(matcher.group(3))) {
					value = matcher.group(1) + expressionValue + matcher.group(3);
				}
				else {
					value = expressionValue;
				}
				matcher = pattern.matcher(value.toString());
			}
		} catch (OgnlException e) {
			throw new ToolException("Failed to evaluate [" + expr + "]", e);
		}
		return value;
	}

	/**
	 * Need to do things like replace the 'context' with "_context"
	 * since OGNL reserves 'context' for internal use and we want to
	 * reference the tool context using it.
	 * 
	 * @param rootObj	root of the OGNL context
	 * @param context	the context map
	 * @return			the final Map required by OGNL.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map buildOgnlContext(Object rootObj, Map<String, Object> context) {
		Map ctx = Ognl.createDefaultContext(rootObj, null,null , new DefaultMemberAccess(true));
		for (Entry<String,Object> e : context.entrySet()) {
			String key = e.getKey().equals("context")? TOOL_CONTEXT_KEY : e.getKey();
			ctx.put(key, e.getValue());
		}
		return ctx;
	}
}
