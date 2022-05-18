// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class IOUtil {
    // not meant to be instantiated
    private IOUtil() {
    }

    public static void readFully(InputStream stream, byte[] bytes, int offset, int length) throws IOException {
        int bytesRead = readRemaining(stream, bytes, offset, length);
        if (bytesRead < length) {
            throw new EOFException(
                "Reached the end of stream with " + (length - bytesRead) + " bytes left to read");
        }
    }


    public static int readRemaining(InputStream stream, byte[] bytes, int offset, int length) throws IOException {
        int pos = offset;
        int remaining = length;
        while (remaining > 0) {
            int bytesRead = stream.read(bytes, pos, remaining);
            if (bytesRead < 0) {
                break;
            }

            remaining -= bytesRead;
            pos += bytesRead;
        }

        return length - remaining;
    }
}