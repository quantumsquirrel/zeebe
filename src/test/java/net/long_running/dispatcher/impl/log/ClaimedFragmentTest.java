package net.long_running.dispatcher.impl.log;

import static net.long_running.dispatcher.impl.log.DataFrameDescriptor.*;
import static org.assertj.core.api.Assertions.*;

import org.junit.Before;
import org.junit.Test;

import net.long_running.dispatcher.ClaimedFragment;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

public class ClaimedFragmentTest
{

    private static final int A_FRAGMENT_LENGTH = 1024;
    UnsafeBuffer underlyingBuffer;
    ClaimedFragment claimedFragment;

    @Before
    public void stetup()
    {
        underlyingBuffer = new UnsafeBuffer(new byte[A_FRAGMENT_LENGTH]);
        claimedFragment = new ClaimedFragment();
    }

    @Test
    public void shouldCommit()
    {
        // given
        claimedFragment.wrap(underlyingBuffer, 0, A_FRAGMENT_LENGTH);

        // if
        claimedFragment.commit();

        // then
        assertThat(underlyingBuffer.getInt(lengthOffset(0))).isEqualTo(A_FRAGMENT_LENGTH - HEADER_LENGTH);
        assertThat(claimedFragment.getOffset()).isEqualTo(HEADER_LENGTH);
        assertThat(claimedFragment.getLength()).isEqualTo(-HEADER_LENGTH);
    }

    @Test
    public void shouldReturnOffsetAndLength()
    {
        // if
        claimedFragment.wrap(underlyingBuffer, 0, A_FRAGMENT_LENGTH);

        // then
        assertThat(claimedFragment.getOffset()).isEqualTo(HEADER_LENGTH);
        assertThat(claimedFragment.getLength()).isEqualTo(A_FRAGMENT_LENGTH - HEADER_LENGTH);
    }

}
