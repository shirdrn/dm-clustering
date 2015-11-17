package org.shirdrn.dm.clustering.common;

import java.util.Map;
import java.util.Set;

public class GenericClusteringResult<P> implements ClusteringResult<P> {

	protected Map<Integer, Set<ClusterPoint<P>>> clusteredPoints;
	
	public GenericClusteringResult() {
		super();
	}

	@Override
	public Map<Integer, Set<ClusterPoint<P>>> getClusteredPoints() {
		return clusteredPoints;
	}

	public void setClusteredPoints(Map<Integer, Set<ClusterPoint<P>>> clusteredPoints) {
		this.clusteredPoints = clusteredPoints;
	}
}
