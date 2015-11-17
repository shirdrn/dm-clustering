package org.shirdrn.dm.clustering.common;

import java.util.Map;
import java.util.Set;

public interface ClusteringResult<P> {

	void setClusteredPoints(Map<Integer, Set<ClusterPoint<P>>> clusteredPoints);
	Map<Integer, Set<ClusterPoint<P>>> getClusteredPoints();
}
