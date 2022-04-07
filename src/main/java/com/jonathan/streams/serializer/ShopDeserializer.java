package com.jonathan.streams.serializer;

import com.google.gson.Gson;
import com.jonathan.dto.ShopDTO;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.Charset;

public class ShopDeserializer implements Deserializer {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    static private Gson gson = new Gson();

    @Override
    public Object deserialize(String s, byte[] bytes) {
        try {
            String shop = new String(bytes, CHARSET);
            return gson.fromJson(shop, ShopDTO.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error reading bytes! ", e);
        }
    }

}
