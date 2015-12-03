package org.shirdrn.dm.clustering.common;


public class CenterPoint extends Point2D implements Comparable<CenterPoint> {
	
	private Integer id;
	private final Point2D point;

	public CenterPoint(Double x, Double y) {
		super(x, y);
		point = new Point2D(x, y);
	}

	public CenterPoint(Integer id, Point2D point) {
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
	public int compareTo(CenterPoint o) {
		double diff = this.id - o.id;
		return diff<0 ? -1 : (diff>0 ? 1 : 0);
	}
	
	@Override
	public int hashCode() {
		return super.hashCode() + 31 * id.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		CenterPoint other = (CenterPoint) obj;
		return super.equals(obj) && this.id == other.id;
	}
	
	@Override
	public String toString() {
		return id + "=>" + super.toString();
	}
}
