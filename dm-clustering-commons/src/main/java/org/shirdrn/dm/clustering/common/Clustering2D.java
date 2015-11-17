package org.shirdrn.dm.clustering.common;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

public abstract class Clustering2D extends AbstractClustering<Point2D> {

	protected final Map<Integer, Set<ClusterPoint<Point2D>>> clusteredPoints = Maps.newHashMap();
	
	public Clustering2D() {
		this(1);
	}
	
	public Clustering2D(int parallism) {
		super(parallism);
		clusteringResult.setClusteredPoints(clusteredPoints);
	}
}
