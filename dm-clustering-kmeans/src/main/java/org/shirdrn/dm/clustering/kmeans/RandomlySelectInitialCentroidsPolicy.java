package org.shirdrn.dm.clustering.kmeans;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.kmeans.common.Centroid;
import org.shirdrn.dm.clustering.kmeans.common.SelectInitialCentroidsPolicy;

import com.google.common.collect.Sets;

public class RandomlySelectInitialCentroidsPolicy implements SelectInitialCentroidsPolicy {

	private final Random random = new Random();
	
	@Override
	public TreeSet<Centroid> select(int k, List<Point2D> points) {
		TreeSet<Centroid> centroids = Sets.newTreeSet();
		Set<Point2D> selectedPoints = Sets.newHashSet();
		while(selectedPoints.size() < k) {
			int index = random.nextInt(points.size());
			Point2D p = points.get(index);
			selectedPoints.add(p);
		}
		
		Iterator<Point2D> iter = selectedPoints.iterator();
		int id = 0;
		while(iter.hasNext()) {
			centroids.add(new Centroid(id++, iter.next()));
		}
		return centroids;
	}

}
