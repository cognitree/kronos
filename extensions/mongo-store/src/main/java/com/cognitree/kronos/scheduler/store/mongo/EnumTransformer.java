package com.cognitree.kronos.scheduler.store.mongo;

import org.bson.Transformer;

/**
 * A standard transformer for serializing/deserializng the enums.
 */
public class EnumTransformer implements Transformer {

    @Override
    public Object transform(Object objectToTransform) {
        return objectToTransform.toString();
    }
}
