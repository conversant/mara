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


/**
 * Manages system console output
 *
 */
public class Console {

	public static final int SILENT = 4;
	public static final int TERSE = 16;
	public static final int VERBOSE = 32;

	private static int mode;

	private Console() {}

	public static void setMode(int mode) {
		Console.mode = mode;
	}

	/**
	 * Prints message to console regardless of mode.
	 * @param message	message to print
	 */
	public static void all(String message) {
		out(TERSE | VERBOSE, message);
	}

	/**
	 * Prints message to console if NOT in SILENT mode.
	 * @param message	message to print
	 */
	public static void out(String message) {
		if (Console.mode != SILENT) {
			System.out.println(message);
		}
	}

	public static void out(int mode, String message) {
		if ((mode & Console.mode) == Console.mode) {
			System.out.println(message);
		}
	}

	public static void error(String message) {
		error(message, null);
	}

	/**
	 *
	 * @param message	message to print
	 * @param t			exception to output as stack trace
	 */
	public static void error(String message, Throwable t) {
		System.err.println(message);
		if (t != null) {
			t.printStackTrace(System.err);
		}
	}

	/**
	 * Convenience method for outputting only in 'sparse'
	 * mode.
	 * @param message	message to print
	 */
	public static void terse(String message) {
		Console.out(TERSE, message);
	}

	/**
	 * Convenience method for outputting only in 'verbose'
	 * mode.
	 * @param message 	message to print
	 */
	public static void verbose(String message) {
		Console.out(VERBOSE, message);
	}

	public static boolean isVerbose() {
		return (VERBOSE & Console.mode) == VERBOSE;
	}

	public static boolean isTerse() {
		return (TERSE & Console.mode) == TERSE;
	}

	public static boolean isSilent() {
		return (SILENT & Console.mode) == SILENT;
	}
}
