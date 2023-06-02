package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.IntValue;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class IntValueCodec implements Codec<IntValue> {
    @Override
    public IntValue decode(BsonReader reader, DecoderContext decoderContext) {
        return new IntValue(reader.readInt32());
    }

    @Override
    public void encode(BsonWriter writer, IntValue value, EncoderContext encoderContext) {
        writer.writeInt32(value.value());
    }

    @Override
    public Class<IntValue> getEncoderClass() {
        return IntValue.class;
    }
}
