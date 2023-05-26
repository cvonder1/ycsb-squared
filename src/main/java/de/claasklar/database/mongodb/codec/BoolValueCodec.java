package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.BoolValue;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class BoolValueCodec implements Codec<BoolValue> {

  @Override
  public BoolValue decode(BsonReader reader, DecoderContext decoderContext) {
    return new BoolValue(reader.readBoolean());
  }

  @Override
  public void encode(BsonWriter writer, BoolValue value, EncoderContext encoderContext) {
    writer.writeBoolean(value.value());
  }

  @Override
  public Class<BoolValue> getEncoderClass() {
    return BoolValue.class;
  }
}
