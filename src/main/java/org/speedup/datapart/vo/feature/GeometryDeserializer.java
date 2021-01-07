package org.speedup.datapart.vo.feature;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class GeometryDeserializer extends StdDeserializer<Geometry<?>> {
    public GeometryDeserializer(Class<?> vc) {
        super(vc);
    }

    public GeometryDeserializer() {
        this(null);
    }

    @Override
    public Geometry<?> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        JsonNode treeNode = jsonParser.getCodec().readTree(jsonParser);
        return null;
    }
}
