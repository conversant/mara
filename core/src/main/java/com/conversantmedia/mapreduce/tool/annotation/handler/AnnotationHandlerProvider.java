package com.conversantmedia.mapreduce.tool.annotation.handler;

import com.conversantmedia.mapreduce.tool.ToolException;

/**
 * Created by pjaromin on 5/21/2015.
 */
public interface AnnotationHandlerProvider {

    Iterable<MaraAnnotationHandler> handlers() throws ToolException;
}
