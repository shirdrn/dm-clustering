package org.shirdrn.dm.clustering.common;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.shirdrn.dm.clustering.common.utils.MetricUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;

public class DistanceCache {

	private final Cache<Set<Point2D>, Double> distanceCache;
	
	public DistanceCache(int cacheSize) {
		Preconditions.checkArgument(cacheSize > 0, "Cache size SHOULD be: cacheSize > 0!");
		distanceCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
	}
	
	public double computeDistance(final Point2D p1, final Point2D p2) {
		Set<Point2D> set = Sets.newHashSet(p1, p2);
		Double distance = 0.0;
		try {
			distance = distanceCache.get(set, new Callable<Double>() {
				@Override
				public Double call() throws Exception {
					return MetricUtils.euclideanDistance(p1, p2);
				}
			});
		} catch (ExecutionException e) {
			Throwables.propagate(e);
		}
		return distance;
	}
}
