package org.camunda.tngp.broker.util.msgpack.value;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.broker.util.msgpack.MsgPackReader;
import org.camunda.tngp.broker.util.msgpack.MsgPackWriter;

public class BinaryValue extends BaseValue
{
    protected final MutableDirectBuffer data = new UnsafeBuffer(0, 0);
    protected int length = 0;

    @Override
    public void reset()
    {
        data.wrap(0, 0);
        length = 0;
    }

    public void wrap(DirectBuffer buff)
    {
        wrap(buff, 0, buff.capacity());
    }

    public void wrap(DirectBuffer buff, int offset, int length)
    {
        this.data.wrap(buff, offset, length);
        this.length = length;
    }

    public void wrap(StringValue decodedKey)
    {
        this.wrap(decodedKey.getValue());
    }

    public int getLength()
    {
        return length;
    }

    public DirectBuffer getValue()
    {
        return data;
    }

    @Override
    public void writeJSON(StringBuilder builder)
    {
        final byte[] bytes = new byte[length];
        data.getBytes(0, bytes);

        builder.append("\"");
        builder.append(new String(Base64.getEncoder().encode(bytes), StandardCharsets.UTF_8));
        builder.append("\"");
    }

    @Override
    public void write(MsgPackWriter writer)
    {
        writer.writeBinary(data);
    }

    @Override
    public void read(MsgPackReader reader)
    {
        final DirectBuffer buffer = reader.getBuffer();
        final int stringLength = reader.readBinaryLength();
        final int offset = reader.getOffset();

        reader.skipBytes(stringLength);

        this.wrap(buffer, offset, stringLength);
    }

    @Override
    public int getEncodedLength()
    {
        return MsgPackWriter.getEncodedBinaryValueLength(length);
    }

}
