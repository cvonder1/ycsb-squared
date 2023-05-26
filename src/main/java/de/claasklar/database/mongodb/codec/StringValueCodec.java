package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.StringValue;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class StringValueCodec implements Codec<StringValue> {

  @Override
  public StringValue decode(BsonReader reader, DecoderContext decoderContext) {
    return new StringValue(reader.readString());
  }

  @Override
  public void encode(BsonWriter writer, StringValue value, EncoderContext encoderContext) {
    writer.writeString(value.value());
  }

  @Override
  public Class<StringValue> getEncoderClass() {
    return StringValue.class;
  }
}
