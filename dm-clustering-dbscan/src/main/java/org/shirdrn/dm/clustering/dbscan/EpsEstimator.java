package org.shirdrn.dm.clustering.dbscan;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dm.clustering.common.DistanceCache;
import org.shirdrn.dm.clustering.common.NamedThreadFactory;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.common.utils.FileUtils;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Estimate value of radius <code>Eps</code> for DBSCAN clustering algorithm.
 *  
 * @author yanjun
 */
public class EpsEstimator {

	private static final Log LOG = LogFactory.getLog(EpsEstimator.class);
	private final List<Point2D> allPoints = Lists.newArrayList();
	private final DistanceCache distanceCache;
	private int k = 4;
	private int parallism = 5;
	private final ExecutorService executorService;
	private CountDownLatch latch;
	private final List<KDistanceCalculator> calculators = Lists.newArrayList();
	private int taskIndex = 0;
	private int calculatorQueueSize = 200;
	private volatile boolean completeToAssignTask = false;
	private boolean isOutputKDsitance = true;
	
	public EpsEstimator() {
		this(4, 5);
	}
	
	public EpsEstimator(int k, int parallism) {
		super();
		this.k = k;
		this.parallism = parallism;
		distanceCache = new DistanceCache(Integer.MAX_VALUE);
		latch = new CountDownLatch(parallism);
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory("KDCALC"));
		LOG.info("Config: k=" + k + ", parallism=" + parallism);
	}
	
	public Iterator<Point2D> allPointIterator() {
		return allPoints.iterator();
	}
	
	public void setOutputKDsitance(boolean isOutputKDsitance) {
		this.isOutputKDsitance = isOutputKDsitance;
	}
	
	public EpsEstimator computeKDistance(File... files) {
			// parse sample files
			FileUtils.read2DPointsFromFiles(allPoints, "[\t,;\\s]+", files);
			// compute k-distance
			try {
				for (int i = 0; i < parallism; i++) {
					KDistanceCalculator calculator = new KDistanceCalculator(calculatorQueueSize);
					calculators.add(calculator);
					executorService.execute(calculator);
					LOG.info("k-distance calculator started: " + calculator);
				}
				
				// convert Point2D to KPoint2D
				for(int i=0; i<allPoints.size(); i++) {
					Point2D p = allPoints.get(i);
					KPoint2D kp = new KPoint2D(p);
					Collections.replaceAll(allPoints, p, kp);
				}
				// assign point tasks
				for(int i=0; i<allPoints.size(); i++) {
					while(true) {
						KDistanceCalculator calculator = getCalculator();
						Task task = new Task((KPoint2D) allPoints.get(i), i);
						if(!calculator.q.offer(task)) {
							continue;
						}
						LOG.debug("Assign Point[" + task.kp + "] to " + calculator);
						break;
					}
				}
				LOG.info("Input: totalPoints=" + allPoints.size());
				
				completeToAssignTask = true;
			} catch(Exception e) {
				throw Throwables.propagate(e);
			} finally {
				try {
					latch.await();
				} catch (InterruptedException e) { }
				LOG.info("Shutdown executor service: " + executorService);
				executorService.shutdown();
			}
		return this;
	}
	
	public void estimateEps() {
		// sort k-distance s
		Collections.sort(allPoints, new Comparator<Point2D>() {

			@Override
			public int compare(Point2D o1, Point2D o2) {
				KPoint2D kp1 = (KPoint2D) o1;
				KPoint2D kp2 = (KPoint2D) o2;
				double diff = kp1.kDistance.doubleValue() - kp2.kDistance.doubleValue();
				if(diff == 0.0) {
					return 0;
				}
				return diff < 0 ? -1 : 1;
			}
			
		});
		
		if(isOutputKDsitance) {
			for(int i=0; i<allPoints.size(); i++) {
				KPoint2D kp = (KPoint2D) allPoints.get(i);
				System.out.println(i + "\t" + kp.kDistance);
			}
		}
	}

	private KDistanceCalculator getCalculator() {
		int index = taskIndex++ % parallism;
		return calculators.get(index);
	}

	private class KDistanceCalculator extends Thread {
		
		private final Log LOG = LogFactory.getLog(KDistanceCalculator.class);
		private final BlockingQueue<Task> q;
		
		public KDistanceCalculator(int qsize) {
			q = new LinkedBlockingQueue<Task>(qsize);
		}
		
		@Override
		public void run() {
			try {
				while(!completeToAssignTask) {
					try {
						while(!q.isEmpty()) {
							Task task = q.poll();
							final KPoint2D p1 = (KPoint2D) task.kp;
							final TreeSet<Double> sortedDistances = Sets.newTreeSet(new Comparator<Double>() {

								@Override
								public int compare(Double o1, Double o2) {
									double diff = o1 - o2;
									if(diff > 0) {
										return -1;
									}
									if(diff < 0) {
										return 1;
									}
									return 0;
								}
								
							});
							for (int i = 0; i < allPoints.size(); i++) {
								if(task.pos != i) {
									final Point2D p2 = allPoints.get(i);
									Double distance = distanceCache.computeDistance((Point2D) p1, (Point2D) p2);
									
									if(!sortedDistances.contains(distance)) {
										sortedDistances.add(distance);
									}
									if(sortedDistances.size() > k) {
										Iterator<Double> iter = sortedDistances.iterator();
										iter.next();
										// remove (k+1)th minimum distance
										iter.remove();
									}
								}
							}
							
							// collect k-distance
							p1.kDistance = sortedDistances.iterator().next();
							LOG.debug("Processed, point=(" + p1 + "), k-distance=" + p1.kDistance);
						}
						Thread.sleep(100);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} finally {
				latch.countDown();
				LOG.info("k-distance calculator exited: " + this);
			}
		}
		
	}
	
	private class Task {
		
		private final KPoint2D kp;
		private final int pos;
		
		public Task(KPoint2D kp, int pos) {
			super();
			this.kp = kp;
			this.pos = pos;
		}
	}
	
	/**
	 * k-distance point
	 * 
	 * @author yanjun
	 */
	private class KPoint2D extends Point2D {

		private Double kDistance = 0.0;
		
		public KPoint2D(Point2D point) {
			super(point.getX(), point.getY());
		}
		
		public KPoint2D(Double x, Double y) {
			super(x, y);
		}
		
		@Override
		public int hashCode() {
			return super.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}
		
	}
	
	public DistanceCache getDistanceCache() {
		return distanceCache;
	}
	
}
