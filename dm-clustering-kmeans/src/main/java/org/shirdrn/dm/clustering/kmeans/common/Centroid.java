package org.shirdrn.dm.clustering.kmeans.common;

import org.shirdrn.dm.clustering.common.Point2D;

public class Centroid extends Point2D implements Comparable<Centroid> {
	
	private Integer id;
	private final Point2D point;

	public Centroid(Double x, Double y) {
		super(x, y);
		point = new Point2D(x, y);
	}

	public Centroid(Integer id, Point2D point) {
		super(point.getX(), point.getY());
		this.id = id;
		this.point = point;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	
	public Point2D toPoint() {
		return point;
	}

	@Override
	public int compareTo(Centroid o) {
		double diff = this.id - o.id;
		return diff<0 ? -1 : (diff>0 ? 1 : 0);
	}
	
	@Override
	public int hashCode() {
		return super.hashCode() + 31 * id.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		Centroid other = (Centroid) obj;
		return super.equals(obj) && this.id == other.id;
	}
	
	@Override
	public String toString() {
		return id + "=>" + super.toString();
	}
}
