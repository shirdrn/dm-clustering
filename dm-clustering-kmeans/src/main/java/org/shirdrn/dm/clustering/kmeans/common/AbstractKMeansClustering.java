package org.shirdrn.dm.clustering.kmeans.common;

import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dm.clustering.common.CenterPoint;
import org.shirdrn.dm.clustering.common.Clustering2D;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.kmeans.utils.RandomlyInitialCenterPointsSelectionPolicy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public abstract class AbstractKMeansClustering extends Clustering2D {

	private static final Log LOG = LogFactory.getLog(AbstractKMeansClustering.class);
	protected final int k;
	protected float maxMovingPointRate;
	protected final int maxIterations;
	protected final List<Point2D> allPoints = Lists.newArrayList();
	protected final TreeSet<CenterPoint> centerPointSet = Sets.newTreeSet();
	protected InitialCenterPointsSelectionPolicy initialCentroidsSelectionPolicy = new RandomlyInitialCenterPointsSelectionPolicy();
	
	public AbstractKMeansClustering(int k, float maxMovingPointRate, int maxIterations, int parallism) {
		super(parallism);
		Preconditions.checkArgument(k > 0, "Required: k > 0!");
		Preconditions.checkArgument(maxMovingPointRate >= 0 && maxMovingPointRate <= 1, "Required: maxMovingPointRate >= 0 && maxMovingPointRate <= 1!");
		Preconditions.checkArgument(maxIterations > 0, "Required: maxIterations > 0!");
		this.k = k;
		this.maxMovingPointRate = maxMovingPointRate;
		this.maxIterations = maxIterations;
		LOG.info("Init: k=" + k + ", maxMovingPointRate=" + maxMovingPointRate + ", parallism=" + parallism + 
				", selectInitialCentroidsPolicy=" + initialCentroidsSelectionPolicy.getClass().getName());
	}

	public void setInitialCentroidsSelectionPolicy(InitialCenterPointsSelectionPolicy initialCentroidsSelectionPolicy) {
		this.initialCentroidsSelectionPolicy = initialCentroidsSelectionPolicy;
		LOG.info("Policy changed: " + this.initialCentroidsSelectionPolicy + " -> " + initialCentroidsSelectionPolicy);
	}

	public TreeSet<CenterPoint> getCenterPointSet() {
		return centerPointSet;
	}
	
}
