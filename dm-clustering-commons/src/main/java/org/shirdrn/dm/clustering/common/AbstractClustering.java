package org.shirdrn.dm.clustering.common;

import java.io.File;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

public abstract class AbstractClustering implements Clustering {

	protected final Map<Point2D, Set<Point2D>> clusteredPoints = Maps.newHashMap();
	protected File[] inputFiles;
	
	@Override
	public int getClusteredCount() {
		return clusteredPoints.size();
	}
	
	@Override
	public void setInputFiles(File... inputFiles) {
		this.inputFiles = inputFiles;		
	}

}
