package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.LongValue;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class LongValueCodec implements Codec<LongValue> {

  @Override
  public LongValue decode(BsonReader reader, DecoderContext decoderContext) {
    return new LongValue(reader.readInt64());
  }

  @Override
  public void encode(BsonWriter writer, LongValue value, EncoderContext encoderContext) {
    writer.writeInt64(value.value());
  }

  @Override
  public Class<LongValue> getEncoderClass() {
    return LongValue.class;
  }
}
