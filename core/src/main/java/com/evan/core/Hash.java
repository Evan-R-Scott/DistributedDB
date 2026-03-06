package com.evan.core;

import java.nio.charset.StandardCharsets;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class Hash {
    private static final HashFunction hash = Hashing.murmur3_32_fixed();

    private static long getHash(String key) {
        return hash.hashString(key, StandardCharsets.UTF_8).asInt() & 0x7fffffff;
    }

    public static int getPartition(String key, int num_partition) {
        return (int) (getHash(key) % num_partition);
    }
}
