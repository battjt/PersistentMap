package net.soliddesign.map;

import java.util.Map;

public interface CloseableMap<K, V> extends Map<K, V>, AutoCloseable {
}