package org.shirdrn.dm.clustering.kmeans.common;

import org.shirdrn.dm.clustering.common.Point2D;

public class Centroid extends Point2D implements Comparable<Centroid> {
	
	private int id;

	public Centroid(Double x, Double y) {
		super(x, y);
	}

	public Centroid(int id, Point2D point) {
		super(point.getX(), point.getY());
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	@Override
	public int compareTo(Centroid o) {
		double diff = this.id - o.id;
		return diff<0 ? -1 : (diff>0 ? 1 : 0);
	}
	
	@Override
	public String toString() {
		return id + "=>" + super.toString();
	}
}
