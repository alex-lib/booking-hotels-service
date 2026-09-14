package com.service.bookinghotels.outbox;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Component
@Slf4j
public class OutboxRelay implements SmartLifecycle {

    private static final int      BATCH_SIZE       = 100;
    private static final Duration POLL_INTERVAL    = Duration.ofSeconds(1);
    private static final Duration LEASE            = Duration.ofSeconds(30);
    private static final Duration INITIAL_BACKOFF  = Duration.ofSeconds(5);
    private static final Duration MAX_BACKOFF      = Duration.ofMinutes(10);
    private static final int      MAX_ATTEMPTS     = 25;
    private static final Duration RETENTION        = Duration.ofDays(7);
    private static final Duration CLEANUP_INTERVAL = Duration.ofMinutes(10);

    private static final RowMapper<OutboxRow> ROW_MAPPER = (rs, rowNum) -> new OutboxRow(
            rs.getObject("id", UUID.class),
            rs.getString("topic"),
            rs.getString("message_key"),
            rs.getString("message_type"),
            rs.getBytes("payload"),
            rs.getInt("attempts"));

    private final JdbcTemplate jdbc;
    private final OutboxPublisher publisher;
    private final String claimant;

    private volatile boolean running;
    private Thread thread;

    public OutboxRelay(JdbcTemplate jdbc, OutboxPublisher publisher) {
        this.jdbc = jdbc;
        this.publisher = publisher;
        this.claimant = hostName() + "-" + UUID.randomUUID();
    }

    private void loop() {
        Instant nextCleanup = Instant.now();
        while (running) {
            try {
                int claimed = pollOnce();
                if (Instant.now().isAfter(nextCleanup)) {
                    cleanupAndReportState();
                    nextCleanup = Instant.now().plus(CLEANUP_INTERVAL);
                }
                if (claimed < BATCH_SIZE) {
                    Thread.sleep(POLL_INTERVAL.toMillis());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("outbox relay cycle failed", e);
            }
        }
    }

    private int pollOnce() {
        List<OutboxRow> batch = claim(claimant, BATCH_SIZE, LEASE);
        Set<String> failedKeys = new HashSet<>();
        for (OutboxRow row : batch) {
            if (row.key() != null && failedKeys.contains(row.key())) {
                reschedule(row, new IllegalStateException("held back: earlier message of key failed"));
                continue;
            }
            try {
                publisher.publish(row);
                markPublished(row.id());
            } catch (Exception e) {
                if (row.key() != null) {
                    failedKeys.add(row.key());
                }
                reschedule(row, e);
            }
        }
        return batch.size();
    }

    private List<OutboxRow> claim(String claimant, int limit, Duration lease) {
        Instant now = Instant.now();
        return jdbc.query("""
                UPDATE outbox SET
                    claimed_by    = ?,
                    claimed_until = ?
                WHERE id IN (
                    SELECT id FROM outbox
                    WHERE status = 'PENDING'
                      AND next_attempt_at <= ?
                      AND (claimed_until IS NULL OR claimed_until <= ?)
                    ORDER BY created_at
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, topic, message_key, message_type, payload, attempts
                """, ROW_MAPPER,
                claimant, Timestamp.from(now.plus(lease)),
                Timestamp.from(now), Timestamp.from(now), limit);
    }

    private void markPublished(UUID id) {
        jdbc.update("""
                UPDATE outbox SET status = 'PUBLISHED', published_at = ?,
                                  claimed_by = NULL, claimed_until = NULL
                WHERE id = ?
                """, Timestamp.from(Instant.now()), id);
    }

    private void reschedule(OutboxRow row, Exception error) {
        int attempts = row.attempts() + 1;
        if (attempts >= MAX_ATTEMPTS) {
            jdbc.update("""
                    UPDATE outbox SET status = 'DEAD', attempts = ?, last_error = ?,
                                      claimed_by = NULL, claimed_until = NULL
                    WHERE id = ?
                    """, attempts, truncate(error.toString()), row.id());
        } else {
            jdbc.update("""
                    UPDATE outbox SET attempts = ?, next_attempt_at = ?,
                                      last_error = ?, claimed_by = NULL, claimed_until = NULL
                    WHERE id = ?
                    """, attempts, Timestamp.from(Instant.now().plus(backoff(attempts))),
                    truncate(error.toString()), row.id());
        }
    }

    private static Duration backoff(int attempts) {
        Duration backoff = INITIAL_BACKOFF.multipliedBy(1L << Math.min(attempts - 1, 20));
        return backoff.compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff;
    }

    private void cleanupAndReportState() {
        int removed = jdbc.update(
                "DELETE FROM outbox WHERE status = 'PUBLISHED' AND published_at < ?",
                Timestamp.from(Instant.now().minus(RETENTION)));
        if (removed > 0) {
            log.info("outbox cleanup removed {} published messages", removed);
        }
        Long lagSeconds = jdbc.queryForObject(
                "SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::bigint"
                        + " FROM outbox WHERE status = 'PENDING'", Long.class);
        Long dead = jdbc.queryForObject(
                "SELECT count(*) FROM outbox WHERE status = 'DEAD'", Long.class);
        if (dead != null && dead > 0) {
            log.error("outbox has {} DEAD messages, manual investigation required (see last_error)", dead);
        }
        log.info("outbox state: delivery lag {}s, DEAD {}", lagSeconds, dead);
    }

    private static String truncate(String error) {
        return error.length() > 1000 ? error.substring(0, 1000) : error;
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }

    @Override
    public void start() {
        running = true;
        thread = new Thread(this::loop, "outbox-relay");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void stop() {
        running = false;
        thread.interrupt();
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
