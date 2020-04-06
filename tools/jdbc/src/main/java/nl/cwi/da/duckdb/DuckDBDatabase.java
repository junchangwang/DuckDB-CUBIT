package nl.cwi.da.duckdb;

import java.nio.ByteBuffer;

public class DuckDBDatabase {

	static {
		System.loadLibrary("duckdb_java");
	}
	
	// We use ByteBuffer as a cheap way to pass pointers back and forth
	// results db_ref database reference object
	protected static native ByteBuffer duckdb_jdbc_startup(String path, boolean read_only);
	protected static native void duckdb_jdbc_shutdown(ByteBuffer db_ref);

	// returns conn_ref connection reference object
	protected static native ByteBuffer duckdb_jdbc_connect(ByteBuffer db_ref);
	protected static native void duckdb_jdbc_disconnect(ByteBuffer conn_ref);

	// returns res_ref result reference object
	protected static native ByteBuffer duckdb_jdbc_prepare(ByteBuffer conn_ref, String query);
	protected static native ByteBuffer duckdb_jdbc_execute(ByteBuffer stmt_ref, Object[] params);
	protected static native void duckdb_jdbc_release(ByteBuffer stmt_ref);
	
	protected static native void duckdb_jdbc_free_result(ByteBuffer res_ref);
	
	// TODO result fetching stuff


	// TODO how do we clean this up again??
	public DuckDBDatabase(String url) {
		db_ref = duckdb_jdbc_startup(url, false);

	}
	
	protected ByteBuffer db_ref;

}
