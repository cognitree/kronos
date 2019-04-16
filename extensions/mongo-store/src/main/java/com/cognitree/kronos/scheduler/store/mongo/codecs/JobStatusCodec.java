package com.cognitree.kronos.scheduler.store.mongo.codecs;

import com.cognitree.kronos.scheduler.model.Job;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class JobStatusCodec implements Codec<Job.Status> {

    @Override
    public void encode(BsonWriter writer, Job.Status value, EncoderContext encoderContext) {
        writer.writeString(value.name());
    }

    @Override
    public Job.Status decode(BsonReader reader, DecoderContext decoderContext) {
        return Job.Status.valueOf(reader.readString());
    }

    @Override
    public Class<Job.Status> getEncoderClass() {
        return Job.Status.class;
    }
}
