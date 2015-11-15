package org.shirdrn.dm.clustering.kmeans.common;

import java.util.List;
import java.util.Set;

import org.shirdrn.dm.clustering.common.Point2D;

public interface SelectInitialCentroidsPolicy {

	Set<Point2D> select(int k, List<Point2D> points);
}
