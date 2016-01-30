package net.soliddesign.map;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;

public class BigByteBuffer implements AutoCloseable {
	private long position;
	/** length of data, not length of file */
	private long length;
	final private RandomAccessFile file;
	// Each map is 2G, overlapping mapped at every 1G, so the largest object
	// should be 1G
	private ArrayList<MappedByteBuffer> buffers = new ArrayList<MappedByteBuffer>();

	public BigByteBuffer(File fileName) throws FileNotFoundException {
		file = new RandomAccessFile(fileName, "rw");
	}

	public void putInt(int value) {
		getBuffer(4).putInt(value);
	}

	public void putShort(short value) {
		getBuffer(2).putShort(value);
	}

	public ByteBuffer getBuffer(int size) {
		try {
			if (position + size > file.length()) {
				long newLength = (Long.highestOneBit(position + size) << 1) - 1;
				file.seek(newLength);
				file.write(0);
				if (!buffers.isEmpty())
					buffers.remove(buffers.size() - 1);
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to resize.", e);
		}
		int offset = (int) (position >> 30);
		MappedByteBuffer buf = offset < buffers.size() ? buffers.get(offset) : null;
		if (buf == null) {
			try {
				int start = offset << 30;
				buf = file.getChannel().map(MapMode.READ_WRITE, start, Math.min(file.length() - start, 0x40000000));
				while (buffers.size() <= offset) {
					buffers.add(null);
				}
				buffers.set(offset, buf);
			} catch (IOException e) {
				throw new RuntimeException("Failed to map file.", e);
			}
		}
		buf.position(0x7FFFFFFF & (int) position);
		position += size;
		length=Math.max(length, position);
		return buf;

	}

	public void putByte(byte value) {
		getBuffer(1).put(value);
	}

	public long position() {
		return position;
	}

	public long getLong() {
		return getBuffer(8).getLong();
	}

	public void position(long p) {
		position = p;
	}

	public void putBytes(byte[] bytes) {
		getBuffer(bytes.length).put(bytes);
	}

	public void putLong(long value) {
		getBuffer(8).putLong(value);
	}

	public byte[] getBytes(int size) {
		byte[] buf = new byte[size];
		getBuffer(size).get(buf);
		return buf;
	}

	public long length() {
		return length;
	}

	public short getShort() {
		return getBuffer(2).getShort();
	}

	public int getInt() {
		return getBuffer(4).getInt();
	}

	@Override
	public void close() throws IOException {
		file.close();
	}

}
