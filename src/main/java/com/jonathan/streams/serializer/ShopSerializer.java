package com.jonathan.streams.serializer;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;

public class ShopSerializer implements Serializer {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    static private Gson gson = new Gson();

    @Override
    public byte[] serialize(String s, Object o) {
        String line = gson.toJson(o);
        return line.getBytes(CHARSET);
    }

}
