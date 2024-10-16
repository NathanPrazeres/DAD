package dadkvs.server.domain;

public class TransactionRecord {
	private int timestamp;
	private int read1Key;
	private int read1Version;
	private int read2Key;
	private int read2Version;
	private int prepareKey;
	private int prepareValue;

	public TransactionRecord() {
		this.read1Key = 0;
		this.read1Version = 0;
		this.read2Key = 0;
		this.read2Version = 0;
		this.prepareKey = 0;
		this.prepareValue = 0;
		this.timestamp = -1;
	}

	public TransactionRecord(final int key1, final int v1, final int key2, final int v2, final int wkey, final int wval) {
		this.read1Key = key1;
		this.read1Version = v1;
		this.read2Key = key2;
		this.read2Version = v2;
		this.prepareKey = wkey;
		this.prepareValue = wval;
		this.timestamp = -1;
	}

	public TransactionRecord(final int key1, final int v1, final int key2, final int v2, final int wkey, final int wval,
			final int ts) {
		this.read1Key = key1;
		this.read1Version = v1;
		this.read2Key = key2;
		this.read2Version = v2;
		this.prepareKey = wkey;
		this.prepareValue = wval;
		this.timestamp = ts;
	}

	// Getter and Setter methods for all fields
	public int getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(final int timestamp) {
		this.timestamp = timestamp;
	}

	public int getRead1Key() {
		return read1Key;
	}

	public void setRead1Key(final int read1Key) {
		this.read1Key = read1Key;
	}

	public int getRead1Version() {
		return read1Version;
	}

	public void setRead1Version(final int read1Version) {
		this.read1Version = read1Version;
	}

	public int getRead2Key() {
		return read2Key;
	}

	public void setRead2Key(final int read2Key) {
		this.read2Key = read2Key;
	}

	public int getRead2Version() {
		return read2Version;
	}

	public void setRead2Version(final int read2Version) {
		this.read2Version = read2Version;
	}

	public int getPrepareKey() {
		return prepareKey;
	}

	public void setPrepareKey(final int prepareKey) {
		this.prepareKey = prepareKey;
	}

	public int getPrepareValue() {
		return prepareValue;
	}

	public void setPrepareValue(final int prepareValue) {
		this.prepareValue = prepareValue;
	}
}
