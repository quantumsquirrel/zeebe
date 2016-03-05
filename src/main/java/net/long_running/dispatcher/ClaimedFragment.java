package net.long_running.dispatcher;

import static net.long_running.dispatcher.impl.log.DataFrameDescriptor.*;

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * Represents a claimed fragment in the buffer.
 *
 * Reusable but not threadsafe.
 *
 */
public class ClaimedFragment
{

    protected final UnsafeBuffer buffer;

    public ClaimedFragment()
    {
        buffer = new UnsafeBuffer(0,0);
    }

    public void wrap(UnsafeBuffer underlyingbuffer, int fragmentOffset, int fragmentLength)
    {
        buffer.wrap(underlyingbuffer, fragmentOffset, fragmentLength);
    }

    public int getOffset()
    {
        return HEADER_LENGTH;
    }

    public int getLength()
    {
        return buffer.capacity() - HEADER_LENGTH;
    }

    public MutableDirectBuffer getBuffer()
    {
        return buffer;
    }

    public void commit()
    {
        // commit the message by writing the positive length
        buffer.putIntOrdered(0, buffer.capacity() - HEADER_LENGTH);
        reset(buffer);
    }

    private static void reset(UnsafeBuffer fragmentWrapper)
    {
        fragmentWrapper.wrap(0,0);
    }

}
