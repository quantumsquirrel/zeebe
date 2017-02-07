/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.tngp.broker.taskqueue.processor;

import static org.camunda.tngp.protocol.clientapi.EventType.TASK_EVENT;
import static org.camunda.tngp.util.EnsureUtil.ensureGreaterThan;
import static org.camunda.tngp.util.EnsureUtil.ensureNotNull;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.camunda.tngp.broker.Constants;
import org.camunda.tngp.broker.logstreams.processor.BrokerStreamProcessor;
import org.camunda.tngp.broker.logstreams.processor.NoopSnapshotSupport;
import org.camunda.tngp.broker.taskqueue.data.TaskEvent;
import org.camunda.tngp.broker.taskqueue.data.TaskEventType;
import org.camunda.tngp.logstreams.log.LogStreamWriter;
import org.camunda.tngp.logstreams.log.LoggedEvent;
import org.camunda.tngp.logstreams.processor.EventProcessor;
import org.camunda.tngp.logstreams.processor.StreamProcessorCommand;
import org.camunda.tngp.logstreams.processor.StreamProcessorContext;
import org.camunda.tngp.logstreams.spi.SnapshotSupport;
import org.camunda.tngp.util.buffer.BufferUtil;
import org.camunda.tngp.util.time.ClockUtil;

public class LockTaskStreamProcessor extends BrokerStreamProcessor
{
    protected final LockTaskEventProcessor lockTaskEventProcessor = new LockTaskEventProcessor();

    protected final NoopSnapshotSupport noopSnapshotSupport = new NoopSnapshotSupport();
    protected ManyToOneConcurrentArrayQueue<StreamProcessorCommand> cmdQueue;

    protected final Long2ObjectHashMap<TaskSubscription> subscriptionsById = new Long2ObjectHashMap<>();
    protected Iterator<TaskSubscription> subscriptionIterator;

    protected final DirectBuffer subscriptedTaskType;

    protected int availableSubscriptionCredits = 0;

    protected final TaskEvent taskEvent = new TaskEvent();
    protected long eventPosition = 0;
    protected long eventKey = 0;

    // activate the processor while adding the first subscription
    protected boolean isSuspended = true;

    public LockTaskStreamProcessor(DirectBuffer subscriptedTaskType)
    {
        super(TASK_EVENT);

        this.subscriptedTaskType = subscriptedTaskType;
        this.subscriptionIterator = subscriptionsById.values().iterator();
    }

    @Override
    public SnapshotSupport getStateResource()
    {
        // need the snapshot resource to restore the log position
        return noopSnapshotSupport;
    }

    @Override
    public boolean isSuspended()
    {
        return isSuspended;
    }

    public DirectBuffer getSubscriptedTaskType()
    {
        return subscriptedTaskType;
    }

    @Override
    public void onOpen(StreamProcessorContext context)
    {
        cmdQueue = context.getStreamProcessorCmdQueue();
    }

    protected <T> CompletableFuture<T> addCommand(Consumer<CompletableFuture<T>> action)
    {
        final CompletableFuture<T> future = new CompletableFuture<>();

        cmdQueue.add(() -> action.accept(future));

        return future;
    }

    public CompletableFuture<Void> addSubscription(TaskSubscription subscription)
    {
        ensureNotNull("subscription", subscription);
        ensureNotNull("lock task type", subscription.getLockTaskType());
        ensureGreaterThan("subscription credits", subscription.getCredits(), 0);

        if (!BufferUtil.equals(subscription.getLockTaskType(), subscriptedTaskType))
        {
            final String errorMessage = String.format("Subscription task type is not equal to '%s'.", BufferUtil.bufferAsString(subscriptedTaskType));
            throw new RuntimeException(errorMessage);
        }

        return addCommand(future ->
        {
            subscriptionsById.put(subscription.getId(), subscription);

            availableSubscriptionCredits += subscription.getCredits();

            isSuspended = false;
        });
    }

    public CompletableFuture<Boolean> removeSubscription(long subscriptionId)
    {
        return addCommand(future ->
        {
            final TaskSubscription subscription = subscriptionsById.remove(subscriptionId);
            if (subscription != null)
            {
                availableSubscriptionCredits -= subscription.getCredits();
            }

            final boolean hasSubscriptions = subscriptionsById.size() > 0;
            if (!hasSubscriptions)
            {
                isSuspended = true;
            }

            future.complete(hasSubscriptions);
        });
    }

    public CompletableFuture<Void> updateSubscriptionCredits(long subscriptionId, int credits)
    {
        ensureGreaterThan("subscription credits", credits, 0);

        return addCommand(future ->
        {
            final TaskSubscription subscription = subscriptionsById.get(subscriptionId);
            if (subscription != null)
            {
                availableSubscriptionCredits += credits - subscription.getCredits();
                subscription.setCredits(credits);

                isSuspended = false;

                future.complete(null);
            }
            else
            {
                final String errorMessage = String.format("Subscription with id '%s' not found.", subscriptionId);
                future.completeExceptionally(new RuntimeException(errorMessage));
            }
        });
    }

    protected TaskSubscription getNextAvailableSubscription()
    {
        TaskSubscription nextSubscription = null;

        if (availableSubscriptionCredits > 0)
        {
            final int subscriptionSize = subscriptionsById.size();
            int seenSubscriptions = 0;

            while (seenSubscriptions < subscriptionSize && nextSubscription == null)
            {
                if (!subscriptionIterator.hasNext())
                {
                    // assuming that it just reset the existing iterator internally
                    subscriptionIterator = subscriptionsById.values().iterator();
                }

                final TaskSubscription subscription = subscriptionIterator.next();
                if (subscription.getCredits() > 0)
                {
                    nextSubscription = subscription;
                }

                seenSubscriptions += 1;
            }
        }
        return nextSubscription;
    }

    @Override
    public EventProcessor onCheckedEvent(LoggedEvent event)
    {
        eventPosition = event.getPosition();
        eventKey = event.getLongKey();

        taskEvent.reset();
        event.readValue(taskEvent);

        EventProcessor eventProcessor = null;

        if (BufferUtil.equals(taskEvent.getType(), subscriptedTaskType))
        {
            switch (taskEvent.getEventType())
            {
                case CREATED:
                case LOCK_EXPIRED:
                    eventProcessor = lockTaskEventProcessor;
                    break;

                default:
                    break;
            }
        }
        return eventProcessor;
    }

    class LockTaskEventProcessor implements EventProcessor
    {
        protected boolean hasLockedTask;
        protected TaskSubscription lockSubscription;

        @Override
        public void processEvent()
        {
            hasLockedTask = false;

            lockSubscription = getNextAvailableSubscription();
            if (lockSubscription != null)
            {
                final long lockTimeout = ClockUtil.getCurrentTimeInMillis() + lockSubscription.getLockTime();

                taskEvent
                    .setEventType(TaskEventType.LOCK)
                    .setLockTime(lockTimeout);

                hasLockedTask = true;
            }
        }

        @Override
        public long writeEvent(LogStreamWriter writer)
        {
            long position = 0;

            if (hasLockedTask)
            {
                targetEventMetadata.reset();

                targetEventMetadata
                    .reqChannelId(lockSubscription.getChannelId())
                    .subscriptionId(lockSubscription.getId())
                    .protocolVersion(Constants.PROTOCOL_VERSION)
                    .eventType(TASK_EVENT);
                // TODO: targetEventMetadata.raftTermId(raftTermId);

                position = writer.key(eventKey)
                        .metadataWriter(targetEventMetadata)
                        .valueWriter(taskEvent)
                        .tryWrite();
            }
            return position;
        }

        @Override
        public void updateState()
        {
            if (hasLockedTask)
            {
                final int credits = lockSubscription.getCredits();
                lockSubscription.setCredits(credits - 1);

                availableSubscriptionCredits -= 1;

                if (availableSubscriptionCredits <= 0)
                {
                    isSuspended = true;
                }
            }
        }
    }

}