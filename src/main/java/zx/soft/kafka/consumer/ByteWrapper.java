package zx.soft.kafka.consumer;

import java.util.Arrays;

public class ByteWrapper {

	private byte[] bytes;

	public ByteWrapper(byte[] bytes) {
		this.bytes = bytes;
	}
	
	public byte[] getBytes() {
		return bytes;
	}

	public void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(bytes);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ByteWrapper other = (ByteWrapper) obj;
		if (!Arrays.equals(bytes, other.bytes))
			return false;
		return true;
	}
	
}
