package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.primitives.document.Value;
import java.util.HashMap;
import java.util.Map;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

public class OurDocumentCodec implements Codec<OurDocument> {

  private static final String ID_FIELD_NAME = "_id";
  private final CodecRegistry codecRegistry;
  private final ObjectValueEncoder encoder;
  private final BsonTypeValueCodecMap codecMap;

  public OurDocumentCodec(CodecRegistry codecRegistry, BsonTypeValueCodecMap codecMap) {
    this.codecRegistry = codecRegistry;
    this.encoder = new ObjectValueEncoder(codecRegistry);
    this.codecMap = codecMap;
  }

  @Override
  public OurDocument decode(BsonReader reader, DecoderContext decoderContext) {
    Id id = null;
    Map<String, Value> values = new HashMap<>();

    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      String fieldName = reader.readName();
      if (fieldName.equals(ID_FIELD_NAME)) {
        id = readId(reader, decoderContext);
      } else {
        values.put(fieldName, ContainerCodecHelper.readValue(reader, decoderContext, codecMap));
      }
    }
    reader.readEndDocument();

    if (id == null) {
      throw new IllegalArgumentException("id must exists and needs to be not null");
    }

    return new OurDocument(id, values);
  }

  @Override
  public void encode(BsonWriter writer, OurDocument value, EncoderContext encoderContext) {
    encoder.encode(writer, value, encoderContext);
  }

  @Override
  public Class<OurDocument> getEncoderClass() {
    return OurDocument.class;
  }

  private Id readId(BsonReader reader, DecoderContext decoderContext) {
    return decoderContext.decodeWithChildContext(codecRegistry.get(Id.class), reader);
  }
}
