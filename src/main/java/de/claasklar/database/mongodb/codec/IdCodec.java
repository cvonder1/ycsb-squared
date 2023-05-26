package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.Id;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.ObjectId;

public class IdCodec implements Codec<Id> {

  @Override
  public Id decode(BsonReader reader, DecoderContext decoderContext) {
    return new Id(reader.readObjectId().toByteArray());
  }

  @Override
  public void encode(BsonWriter writer, Id value, EncoderContext encoderContext) {
    writer.writeObjectId(new ObjectId(value.id()));
  }

  @Override
  public Class<Id> getEncoderClass() {
    return Id.class;
  }
}
