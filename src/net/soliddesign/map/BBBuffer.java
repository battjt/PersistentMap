package net.soliddesign.map;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;

public class BBBuffer implements AutoCloseable {
	public static BBBuffer create(File fileName) throws IOException {
		return new BBBuffer(fileName);
	}

	/** length of file */
	private long fileLength;

	/** underlying file or something */
	final private RandomAccessFile file;

	/**
	 * Each map is 2G, overlapping mapped at every 1G, so the largest object can
	 * be 1G
	 */
	private ArrayList<MappedByteBuffer> buffers = new ArrayList<>();

	/** position within file */
	protected long position;

	/** length of data, not length of file */
	protected long length;

	public BBBuffer(File fileName) throws IOException {
		super();
		file = new RandomAccessFile(fileName, "rw");
		fileLength = file.length();
	}

	@Override
	public void close() throws IOException {
		for (MappedByteBuffer bb : buffers) {
			bb.force();
		}
		file.close();
	}

	public ByteBuffer getBuffer() {
		return getBuffer(getInt());
	}

	public ByteBuffer getBuffer(int size) {
		MappedByteBuffer buf = pageBuffer(size);
		ByteBuffer slice = buf.slice();
		slice.limit(size);
		return slice;
	}

	public int getInt() {
		return pageBuffer(Integer.BYTES).getInt();
	}

	public long getLong() {
		return pageBuffer(Long.BYTES).getLong();
	}

	public long length() {
		return length;
	}

	private MappedByteBuffer pageBuffer(int size) {
		// System.err.format("getBuffer(%x) at %x\n", size, position);
		try {
			if (position + size > fileLength) {
				// FIXME why -1?
				fileLength = (Long.highestOneBit(position + size) << 1) - 1;
				System.err.format("resize: %x getBuffer(%x) at %x\n", fileLength, size, position);
				file.seek(fileLength - 1);
				file.write(0);
				if (!buffers.isEmpty()) {
					buffers.get(buffers.size() - 1).force();
					buffers.set(buffers.size() - 1, null);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to resize.", e);
		}
		int offset = (int) (position >> 30);
		MappedByteBuffer buf = offset < buffers.size() ? buffers.get(offset) : null;
		if (buf == null) {
			try {
				int start = offset << 30;
				long bufferSize = Math.min(fileLength - start, 0x40000000);
				System.err.println("BigBuffer.create:" + start + " - " + bufferSize);
				buf = file.getChannel().map(MapMode.READ_WRITE, start, bufferSize);
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
		length = Math.max(length, position);
		return buf;
	}

	public long position() {
		return position;
	}

	public void position(long p) {
		position = p;
	}

	public void putBuffer(ByteBuffer b) {
		b.rewind();
		getBuffer(Integer.BYTES + b.remaining()).putInt(b.remaining()).put(b);
	}

	public void putInt(int value) {
		getBuffer(Integer.BYTES).putInt(value);
	}

	public void putLong(long value) {
		getBuffer(Long.BYTES).putLong(value);
	}

	public void skip() {
		position += getInt() + Integer.BYTES;
	}

}
