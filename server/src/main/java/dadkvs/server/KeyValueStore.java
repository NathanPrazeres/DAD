package dadkvs.server;

public class KeyValueStore {
	private final int size;
	private final VersionedValue[] values;

	public KeyValueStore(final int n_entries) {
		this.size = n_entries;
		this.values = new VersionedValue[n_entries];
		for (int i = 0; i < n_entries; i++) {
			this.values[i] = new VersionedValue(0, 0);
		}
	}

	synchronized public VersionedValue read(final int k) {
		if (k < size) {
			return values[k];
		} else {
			return null;
		}
	}

	synchronized public boolean write(final int k, final VersionedValue v) {
		if (k < size) {
			values[k] = v;
			return true;
		} else
			return false;
	}

	synchronized public boolean commit(final TransactionRecord tr) {
		System.out.println("store commit read first key = " + tr.getRead1Key() + " with version = " + tr.getRead1Version()
				+ "  and current version = " + this.read(tr.getRead1Key()).getVersion());
		System.out.println("store commit read second key = " + tr.getRead2Key() + " with version = " + tr.getRead2Version()
				+ " and current version = " + this.read(tr.getRead2Key()).getVersion());
		System.out.println("store commit write key  " + tr.getPrepareKey() + " with value = " + tr.getPrepareValue()
				+ " and version " + tr.getTimestamp());
		if (this.read(tr.getRead1Key()).getVersion() == tr.getRead1Version() &&
				this.read(tr.getRead2Key()).getVersion() == tr.getRead2Version()) {
			final VersionedValue vv = new VersionedValue(tr.getPrepareValue(), tr.getTimestamp());
			this.write(tr.getPrepareKey(), vv);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		final StringBuilder vals = new StringBuilder();
		for (int i = 0; i < size; i++) {
			if (values[i].getVersion() != 0) {
				vals.append("Key: " + i + " Value: " + values[i].getValue() + " Version: " + values[i].getVersion() + "\n");
			}
		}
		return vals.toString();
	}
}
