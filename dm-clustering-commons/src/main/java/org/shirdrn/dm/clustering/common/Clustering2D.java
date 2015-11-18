package org.shirdrn.dm.clustering.common;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public abstract class Clustering2D extends AbstractClustering<Point2D> {

	protected final Map<Integer, Set<ClusterPoint<Point2D>>> clusteredPoints = Maps.newHashMap();
	
	public Clustering2D() {
		this(1);
	}
	
	public Clustering2D(int parallism) {
		super(parallism);
		Preconditions.checkArgument(parallism > 0, "Required: parallism > 0!");
		clusteringResult.setClusteredPoints(clusteredPoints);
	}
}
