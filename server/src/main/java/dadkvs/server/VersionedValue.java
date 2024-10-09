package dadkvs.server;

public class VersionedValue {
	private int value;
	private int version;

	public VersionedValue(final int val, final int ver) {
		this.value = val;
		this.version = ver;
	}

	public int getValue() {
		return value;
	}

	public void setValue(final int value) {
		this.value = value;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(final int version) {
		this.version = version;
	}
}
