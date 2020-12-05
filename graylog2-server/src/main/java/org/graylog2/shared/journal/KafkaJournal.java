/**
 * This file is part of Graylog.
 * <p>
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.shared.journal;

import com.codahale.metrics.MetricRegistry;
import com.github.joschi.jadconfig.util.Size;
import com.google.common.util.concurrent.AbstractIdleService;
import kafka.log.LogSegment;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.ThrottleState;
import org.graylog2.plugin.lifecycles.LoadBalancerStatus;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class KafkaJournal extends AbstractIdleService implements Journal {

    public static final long DEFAULT_COMMITTED_OFFSET = Long.MIN_VALUE;
    public static final int NOTIFY_ON_UTILIZATION_PERCENTAGE = 95;
    public static final int THRESHOLD_THROTTLING_DISABLED = -1;
    // Metric names, which should be used twice (once in metric startup and once in metric teardown).
    public static final String METER_WRITTEN_MESSAGES = "writtenMessages";
    public static final String METER_READ_MESSAGES = "readMessages";
    public static final String GAUGE_UNCOMMITTED_MESSAGES = "uncommittedMessages";
    private static final Logger LOG = LoggerFactory.getLogger(KafkaJournal.class);
    private final ServerStatus serverStatus;
    private final int throttleThresholdPercentage;
    private final AtomicReference<ThrottleState> throttleState = new AtomicReference<>();
    private final AtomicLong committedOffset = new AtomicLong(DEFAULT_COMMITTED_OFFSET);
    private volatile boolean shuttingDown;
    private long nextReadOffset = 0L;

    private AtomicLong truncatedOffset = new AtomicLong(0);
    private List<Entry> pendingEntries = Collections.synchronizedList(new ArrayList<Entry>());

    @Inject
    public KafkaJournal(@Named("message_journal_dir") Path journalDirectory,
                        @Named("scheduler") ScheduledExecutorService scheduler,
                        @Named("message_journal_segment_size") Size segmentSize,
                        @Named("message_journal_segment_age") Duration segmentAge,
                        @Named("message_journal_max_size") Size retentionSize,
                        @Named("message_journal_max_age") Duration retentionAge,
                        @Named("message_journal_flush_interval") long flushInterval,
                        @Named("message_journal_flush_age") Duration flushAge,
                        @Named("lb_throttle_threshold_percentage") int throttleThresholdPercentage,
                        MetricRegistry metricRegistry,
                        ServerStatus serverStatus) {

        this(journalDirectory, scheduler, segmentSize, segmentAge, retentionSize, retentionAge, flushInterval, flushAge,
                throttleThresholdPercentage, metricRegistry, serverStatus, KafkaJournal.class.getName());
    }

    /**
     * @param throttleThresholdPercentage The journal utilization percent at which throttling will be triggered.
     *                                    Expressed as an integer between 1 and 100. The value -1 disables throttling.
     */
    public KafkaJournal(Path journalDirectory,
                        ScheduledExecutorService scheduler,
                        Size segmentSize,
                        Duration segmentAge,
                        Size retentionSize,
                        Duration retentionAge,
                        long flushInterval,
                        Duration flushAge,
                        int throttleThresholdPercentage,
                        MetricRegistry metricRegistry,
                        ServerStatus serverStatus,
                        String metricPrefix) {
        this.serverStatus = serverStatus;
        if (throttleThresholdPercentage == THRESHOLD_THROTTLING_DISABLED) {
            this.throttleThresholdPercentage = throttleThresholdPercentage;
        } else {
            this.throttleThresholdPercentage = Integer.min(Integer.max(throttleThresholdPercentage, 0), 100);
        }
    }

    public int getPurgedSegmentsInLastRetention() {
        return 1;
    }

    /**
     * Creates an opaque object which can be passed to {@link #write(java.util.List)} for a bulk journal write.
     *
     * @param idBytes      a byte array which represents the key for the entry
     * @param messageBytes the journal entry's payload, i.e. the message itself
     * @return a journal entry to be passed to {@link #write(java.util.List)}
     */
    @Override
    public Entry createEntry(byte[] idBytes, byte[] messageBytes) {
        return new Entry(idBytes, messageBytes);
    }

    /**
     * Writes the list of entries to the journal.
     *
     * @param entries journal entries to be written
     * @return the last position written to in the journal
     */
    @Override
    public long write(List<Entry> entries) {
        int limit = 100000;
        updateLoadBalancerStatus(pendingEntries.size() * 100 / limit);
        pendingEntries.addAll(entries);
        return truncatedOffset.get() + pendingEntries.size();
    }

    private void updateLoadBalancerStatus(double utilizationPercentage) {
        final LoadBalancerStatus currentStatus = serverStatus.getLifecycle().getLoadbalancerStatus();

        // Flip the status. The next lifecycle events may change status. This should be good enough, because
        // throttling does not offer hard guarantees.
        if (currentStatus == LoadBalancerStatus.THROTTLED && utilizationPercentage < throttleThresholdPercentage) {
            serverStatus.running();
            LOG.info(String.format(Locale.ENGLISH,
                    "Journal usage is %.2f%% (threshold %d%%), changing load balancer status from THROTTLED to ALIVE",
                    utilizationPercentage, throttleThresholdPercentage));
        } else if (currentStatus == LoadBalancerStatus.ALIVE && utilizationPercentage >= throttleThresholdPercentage) {
            serverStatus.throttle();
            LOG.info(String.format(Locale.ENGLISH,
                    "Journal usage is %.2f%% (threshold %d%%), changing load balancer status from ALIVE to THROTTLED",
                    utilizationPercentage, throttleThresholdPercentage));
        }
    }

    /**
     * Writes a single message to the journal and returns the new write position
     *
     * @param idBytes      byte array congaing the message id
     * @param messageBytes encoded message payload
     * @return the last position written to in the journal
     */
    @Override
    public long write(byte[] idBytes, byte[] messageBytes) {
        final Entry journalEntry = createEntry(idBytes, messageBytes);
        return write(Collections.singletonList(journalEntry));
    }

    @Override
    public List<JournalReadEntry> read(long requestedMaximumCount) {
        return read(nextReadOffset, requestedMaximumCount);
    }

    public List<JournalReadEntry> read(long readOffset, long requestedMaximumCount) {
        if (shuttingDown) {
            return Collections.emptyList();
        }

        int indexOffset = Math.toIntExact(readOffset - truncatedOffset.get());
        if (indexOffset < 0) {
            return Collections.emptyList();
        }

        ArrayList<JournalReadEntry> entries = new ArrayList<>();
        for (int i = 0; i < requestedMaximumCount; ++i) {
            int index = indexOffset + i;
            try {
                Entry entry = pendingEntries.get(index);
                entries.add(new JournalReadEntry(entry.getMessageBytes(), readOffset + i));
                nextReadOffset = readOffset + i + 1;
            } catch (IndexOutOfBoundsException e) {
                break;
            }
        }
        return entries;
    }

    /**
     * Upon fully processing, and persistently storing, a batch of messages, the system should mark the message with the
     * highest offset as committed. A background job will write the last position to disk periodically.
     *
     * @param offset the offset of the latest committed message
     */
    @Override
    public void markJournalOffsetCommitted(long offset) {
        long prev;
        do {
            prev = committedOffset.get();
        } while (!committedOffset.compareAndSet(prev, Math.max(offset, prev)));

        cleanupLogs();
    }

    /**
     * A Java transliteration of what the scala implementation does, which unfortunately is declared as private
     */
    protected void flushDirtyLogs() {
    }

    public long getCommittedOffset() {
        return committedOffset.get();
    }

    public long getNextReadOffset() {
        return nextReadOffset;
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        shuttingDown = true;
    }

    public int cleanupLogs() {
        return truncate(committedOffset.get());
    }

    // default visibility for tests
    public Iterable<LogSegment> getSegments() {
        return Collections.emptyList();
    }

    /**
     * Returns the journal size in bytes, exluding index files.
     *
     * @return journal size in bytes
     */
    public long size() {
        int bytesSize = 0;
        for (int i = 0; i < pendingEntries.size(); ++i) {
            try {
                Entry entry = pendingEntries.get(i);
                bytesSize += entry.getMessageBytes().length;
            } catch (IndexOutOfBoundsException e) {
                break;
            }
        }
        return bytesSize;
    }

    /**
     * Returns the number of segments this journal consists of.
     *
     * @return number of segments
     */
    public int numberOfSegments() {
        return 1;
    }

    /**
     * Returns the highest journal offset that has been writting to persistent storage by Graylog.
     * <p>
     * Every message at an offset prior to this one can be considered as processed and does not need to be held in
     * the journal any longer. By default Graylog will try to aggressively flush the journal to consume a smaller
     * amount of disk space.
     * </p>
     *
     * @return the offset of the last message which has been successfully processed.
     */
    public long getCommittedReadOffset() {
        return committedOffset.get();
    }

    /**
     * Discards all data in the journal prior to the given offset.
     *
     * @param offset offset to truncate to, so that no offset in the journal is larger than this.
     */
    public void truncateTo(long offset) {
        truncate(offset);
    }

    private int truncate(long offset) {
        if (offset < this.truncatedOffset.get()) {
            return 0;
        }
        long oldOffset = this.truncatedOffset.getAndSet(offset);
        int reduceSize = Math.toIntExact(offset - oldOffset);

        int count = 0;
        for (int i = 0; i < reduceSize; ++i) {
            try {
                pendingEntries.remove(0);
                ++count;
            } catch (IndexOutOfBoundsException e) {
                break;
            }
        }
        return count;
    }

    /**
     * Returns the first valid offset in the entire journal.
     *
     * @return first offset
     */
    public long getLogStartOffset() {
        return 0;
    }

    /**
     * returns the offset for the next value to be inserted in the entire journal.
     *
     * @return the next offset value (last valid offset is this number - 1)
     */
    public long getLogEndOffset() {
        return truncatedOffset.get() + pendingEntries.size();
    }

    /**
     * For informational purposes this method provides access to the current state of the journal.
     *
     * @return the journal state for throttling purposes
     */
    public ThrottleState getThrottleState() {
        return throttleState.get();
    }

    public void setThrottleState(ThrottleState state) {
        throttleState.set(state);
    }
}
