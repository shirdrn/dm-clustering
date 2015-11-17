package org.shirdrn.dm.clustering.common;

import java.io.File;

public interface Clustering<P> {

	void clustering();
	
	void setInputFiles(File... files);
	
	int getClusteredCount();
	
	ClusteringResult<P> getClusteringResult();
}
