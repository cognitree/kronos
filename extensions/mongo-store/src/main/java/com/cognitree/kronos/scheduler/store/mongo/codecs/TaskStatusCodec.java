package com.cognitree.kronos.scheduler.store.mongo.codecs;

import com.cognitree.kronos.model.Task;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class TaskStatusCodec implements Codec<Task.Status> {

    @Override
    public void encode(BsonWriter writer, Task.Status value, EncoderContext encoderContext) {
        writer.writeString(value.name());
    }

    @Override
    public Task.Status decode(BsonReader reader, DecoderContext decoderContext) {
        return Task.Status.valueOf(reader.readString());
    }

    @Override
    public Class<Task.Status> getEncoderClass() {
        return Task.Status.class;
    }
}
