package com.tm.common.config;

import java.util.Map;

/** Immutable map-backed {@link Config}. */
public final class MapConfig implements Config {

    private final Map<String, String> resolved;

    MapConfig(Map<String, String> resolved) {
        this.resolved = resolved;
    }

    @Override
    public String get(String key) {
        String v = resolved.get(key);
        if (v == null) {
            throw new IllegalArgumentException("unknown config key: " + key);
        }
        return v;
    }
}
