package org.shirdrn.dm.clustering.tool.common;

import java.io.File;

/**
 * Interface to chart XY chart based on clustering result.
 * 
 * @author yanjun
 */
public interface ClusteringXYChart {

	/**
	 * Render XY chart from the given cluster point set.
	 */
	void renderXYChart();
	
	/**
	 * after clustering, we should write generated cluster points to file <code>clusterPointFile</code>, 
	 * and set the cluster point file to display on the XY chart.
	 * @param clusterPointFile
	 */
	void setclusterPointFile(File clusterPointFile);
	
	/**
	 * Sets noise point virtual cluster ID.
	 * @param noisePointClusterId
	 */
	void setNoisePointClusterId(int noisePointClusterId);
	
}
