package test.annotation.handlers;

import com.conversantmedia.mapreduce.tool.AnnotatedTool;
import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.handler.AnnotationHandlerBase;
import com.conversantmedia.mapreduce.tool.annotation.handler.MaraAnnotationHandler;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.stereotype.Service;

import java.lang.annotation.Annotation;

/**
 * Created by pjaromin on 5/21/2015.
 */
public class TestBaseAnnotationHandler extends AnnotationHandlerBase {
    @Override
    public boolean accept(Annotation annotation) throws ToolException {
        return false;
    }

    @Override
    public void process(Annotation annotation, Job job, Object target) throws ToolException {
        // not implemented
    }

    @Override
    public void initialize(AnnotatedTool tool) {
        // not implemented
    }
}
