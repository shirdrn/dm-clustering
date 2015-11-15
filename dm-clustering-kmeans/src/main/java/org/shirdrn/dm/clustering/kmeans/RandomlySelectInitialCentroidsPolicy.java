package org.shirdrn.dm.clustering.kmeans;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.kmeans.common.SelectInitialCentroidsPolicy;

import com.google.common.collect.Sets;

public class RandomlySelectInitialCentroidsPolicy implements SelectInitialCentroidsPolicy {

	private final Random random = new Random();
	
	@Override
	public Set<Point2D> select(int k, List<Point2D> points) {
		Set<Point2D> centroids = Sets.newHashSet();
		for (int i = 0; i < k; i++) {
			int index = random.nextInt(points.size());
			Point2D p = points.get(index);
			while(centroids.contains(p)) {
				index = random.nextInt(points.size());
				p = points.get(index);
			}
			centroids.add(p);
		}
		return centroids;
	}

}
