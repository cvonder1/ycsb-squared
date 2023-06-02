package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.NullValue;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class NullValueCodec implements Codec<NullValue> {
  @Override
  public NullValue decode(BsonReader reader, DecoderContext decoderContext) {
    reader.readNull();
    return NullValue.VALUE;
  }

  @Override
  public void encode(BsonWriter writer, NullValue value, EncoderContext encoderContext) {
    writer.writeNull();
  }

  @Override
  public Class<NullValue> getEncoderClass() {
    return NullValue.class;
  }
}
