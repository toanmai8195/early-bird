package com.tm.common.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic config reader with a fixed precedence: CLI flag (`--key=value`) &gt;
 * environment variable &gt; default. Services declare their keys via the builder
 * and read typed values back.
 *
 * <pre>
 *   Config cfg = Config.from(args)
 *       .with("kafka-brokers", "KAFKA_BROKERS", "localhost:9092")
 *       .with("max-poll-records", "MAX_POLL_RECORDS", "500")
 *       .build();
 *   cfg.get("kafka-brokers");
 *   cfg.getInt("max-poll-records");
 * </pre>
 */
public interface Config {

    String get(String key);

    default int getInt(String key) {
        return Integer.parseInt(get(key));
    }

    default long getLong(String key) {
        return Long.parseLong(get(key));
    }

    default boolean getBool(String key) {
        return Boolean.parseBoolean(get(key));
    }

    /** Starts a builder seeded with CLI overrides parsed from {@code args}. */
    static Builder from(String[] args) {
        return new Builder(parseArgs(args));
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> cli = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--") && arg.contains("=")) {
                int eq = arg.indexOf('=');
                cli.put(arg.substring(2, eq), arg.substring(eq + 1));
            }
        }
        return cli;
    }

    /** Resolves each key as it is declared, applying CLI &gt; env &gt; default. */
    final class Builder {

        private final Map<String, String> cli;
        private final Map<String, String> resolved = new HashMap<>();

        private Builder(Map<String, String> cli) {
            this.cli = cli;
        }

        public Builder with(String key, String envVar, String defaultValue) {
            String fromCli = cli.get(key);
            String fromEnv = System.getenv(envVar);
            String value = fromCli != null
                    ? fromCli
                    : (fromEnv != null && !fromEnv.isEmpty() ? fromEnv : defaultValue);
            resolved.put(key, value);
            return this;
        }

        public Config build() {
            return new MapConfig(Map.copyOf(resolved));
        }
    }
}
