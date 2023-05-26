package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.DoubleValue;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class DoubleValueCodec implements Codec<DoubleValue> {

  @Override
  public DoubleValue decode(BsonReader reader, DecoderContext decoderContext) {
    return new DoubleValue(reader.readDouble());
  }

  @Override
  public void encode(BsonWriter writer, DoubleValue value, EncoderContext encoderContext) {
    writer.writeDouble(value.value());
  }

  @Override
  public Class<DoubleValue> getEncoderClass() {
    return DoubleValue.class;
  }
}
