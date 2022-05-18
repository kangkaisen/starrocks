// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg.io;

import org.apache.iceberg.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public abstract class ByteBufferInputStream extends SeekableInputStream {

    public static ByteBufferInputStream wrap(ByteBuffer... buffers) {
        if (buffers.length == 1) {
            return new SingleBufferInputStream(buffers[0]);
        } else {
            return new MultiBufferInputStream(Arrays.asList(buffers));
        }
    }

    public static ByteBufferInputStream wrap(List<ByteBuffer> buffers) {
        if (buffers.size() == 1) {
            return new SingleBufferInputStream(buffers.get(0));
        } else {
            return new MultiBufferInputStream(buffers);
        }
    }

    public void skipFully(long length) throws IOException {
        long skipped = skip(length);
        if (skipped < length) {
            throw new EOFException(
                "Not enough bytes to skip: " + skipped + " < " + length);
        }
    }

    public abstract int read(ByteBuffer out);

    public abstract ByteBuffer slice(int length) throws EOFException;

    public abstract List<ByteBuffer> sliceBuffers(long length) throws EOFException;

    public ByteBufferInputStream sliceStream(long length) throws EOFException {
        return ByteBufferInputStream.wrap(sliceBuffers(length));
    }

    public abstract List<ByteBuffer> remainingBuffers();

    public ByteBufferInputStream remainingStream() {
        return ByteBufferInputStream.wrap(remainingBuffers());
    }
}
