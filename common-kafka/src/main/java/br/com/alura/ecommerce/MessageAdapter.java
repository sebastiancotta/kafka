package br.com.alura.ecommerce;

import com.google.gson.*;

import java.lang.reflect.Type;

public class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {
    @Override
    public JsonElement serialize(Message message, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();

        jsonObject.addProperty("type", message.getPayload().getClass().getName());
        jsonObject.add("payload", context.serialize(message.getPayload()));
        jsonObject.add("correlationId", context.serialize(message.getId()));

        return jsonObject;
    }

    @Override
    public Message deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        var object = json.getAsJsonObject();
        var payloadType = object.get("type").getAsString();
        var correlationId = (CorrelationId) context.deserialize(object.get("correlationId"), CorrelationId.class);
        try {
            var payload = context.deserialize(object.get("payload"), Class.forName(payloadType));
            return new Message(correlationId, payload);
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }
    }
}
