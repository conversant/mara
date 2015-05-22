package test.annotation.handlers;

import org.springframework.stereotype.Service;

/**
 * Created by pjaromin on 5/21/2015.
 */
@Service
public class Last1TestAnnotationHandler extends TestBaseAnnotationHandler {

    @Override
    public boolean runLast() { return true; }
}
