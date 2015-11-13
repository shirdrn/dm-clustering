package org.shirdrn.dm.clustering.common.utils;

import java.io.Closeable;

public class FileUtils {

	public static void closeQuietly(Closeable... closeables) {
		if(closeables != null) {
			for(Closeable closeable : closeables) {
				try {
					closeable.close();
				} catch (Exception e) { }
			}
		}
	}
}
