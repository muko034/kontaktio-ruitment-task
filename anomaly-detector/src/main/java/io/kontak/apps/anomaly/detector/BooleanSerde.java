package io.kontak.apps.anomaly.detector;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class BooleanSerde extends Serdes.WrapperSerde<Boolean> {

    public BooleanSerde() {
        super(new BooleanSerializer(), new BooleanDeserializer());
    }
}

class BooleanSerializer implements Serializer<Boolean> {

    @Override
    public byte[] serialize(String topic, Boolean data) {
        return new byte[data ? 1 : 0];
    }
}

class BooleanDeserializer implements Deserializer<Boolean> {

    @Override
    public Boolean deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return data[0] != 0;
    }
}
