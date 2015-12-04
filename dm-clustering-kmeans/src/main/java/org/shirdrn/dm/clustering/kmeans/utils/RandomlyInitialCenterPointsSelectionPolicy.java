package org.shirdrn.dm.clustering.kmeans.utils;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.shirdrn.dm.clustering.common.CenterPoint;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.kmeans.common.InitialCenterPointsSelectionPolicy;

import com.google.common.collect.Sets;

public class RandomlyInitialCenterPointsSelectionPolicy implements InitialCenterPointsSelectionPolicy {

	private final Random random = new Random();
	
	@Override
	public TreeSet<CenterPoint> select(int k, List<Point2D> points) {
		TreeSet<CenterPoint> centroids = Sets.newTreeSet();
		Set<Point2D> selectedPoints = Sets.newHashSet();
		while(selectedPoints.size() < k) {
			int index = random.nextInt(points.size());
			Point2D p = points.get(index);
			selectedPoints.add(p);
		}
		
		Iterator<Point2D> iter = selectedPoints.iterator();
		int id = 0;
		while(iter.hasNext()) {
			centroids.add(new CenterPoint(id++, iter.next()));
		}
		return centroids;
	}

}
