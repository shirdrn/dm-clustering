package org.shirdrn.dm.clustering.tool.utils;

import java.awt.Color;
import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import javax.swing.AbstractAction;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;

import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.shirdrn.dm.clustering.common.ClusterPoint2D;
import org.shirdrn.dm.clustering.tool.common.ClusteringXYChart;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ChartUtils {

	private static final Random RAND = new Random();
	
	/**
	 * Create a {@link XYSeriesCollection} object to render XY chart, and {@link XYSeriesCollection} object 
	 * contains clustered points except noise point set .
	 * @param clusterPoints
	 * @return
	 */
	public static XYSeriesCollection createXYSeriesCollection(final Map<Integer, Set<ClusterPoint2D>> clusterPoints) {
		XYSeriesCollection xySeriesCollection = new XYSeriesCollection();
		Iterator<Entry<Integer, Set<ClusterPoint2D>>> iter = clusterPoints.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Integer, Set<ClusterPoint2D>> entry = iter.next();
			int clusterId = entry.getKey();
			Set<ClusterPoint2D> clusteredPoints = entry.getValue();
			XYSeries xySeries = new XYSeries(clusterId);
			for(ClusterPoint2D cp : clusteredPoints) {
				xySeries.add(cp.getPoint().getX(), cp.getPoint().getY());
			}
			xySeriesCollection.addSeries(xySeries);
		}
		return xySeriesCollection;
	}
	
	/**
	 * Generate colors for XY-chart, as well as a color space. And:
	 * <ol>
	 * 	<li>{@linkplain clusterIdSet.size() == xyColors.size()}</li>
	 * <li>{@linkplain colorSpace.sizeclusterIdSet.size() == xyColors.size()}</li>
	 * </ol>
	 * @param clusterIdSet
	 * @param colorSpace
	 * @return
	 */
	public static Map<Integer, Color> generateXYColors(final Set<Integer> clusterIdSet, final List<Color> colorSpace) {
		Map<Integer, Color> xyColors = Maps.newHashMap();
		int numClusters = clusterIdSet.size();
		List<Integer> rValues = Lists.newArrayList();
		List<Integer> gValues = Lists.newArrayList();
		List<Integer> bValues = Lists.newArrayList();
		int i = 1;
		for(; i<Integer.MAX_VALUE; i++) {
			int product = i * i * i;
			if(product > numClusters * 3 + i) {
				break;
			}
		}
		
		int step = 255 / i;
		generateColorValues(step, rValues);
		generateColorValues(step, gValues);
		generateColorValues(step, bValues);
		
		for (int rIndex = 0; rIndex < rValues.size(); rIndex++) {
			for (int gIndex = 0; gIndex < rValues.size(); gIndex++) {
				for (int bIndex = 0; bIndex < rValues.size(); bIndex++) {
					colorSpace.add(new Color(rValues.get(rIndex), gValues.get(gIndex), bValues.get(bIndex)));
				}
			}
		}
		
		for (int clusterId : clusterIdSet) {
			Color color = selectColorRandomly(colorSpace);
			xyColors.put(clusterId, color);
		}
		return xyColors;
	}
	
	public static Color selectColorRandomly(final List<Color> colorSpace) {
		int index = RAND.nextInt(colorSpace.size());
		return colorSpace.remove(index);
	}
	
	private static void generateColorValues(int step, List<Integer> rgbValues) {
		for (int j = 0; j < 255; j+=step) {
			rgbValues.add(j);
		}		
	}
	
	/**
	 * A driver to generate XY chart.
	 * @param chart
	 */
	public static void generateXYChart(final ClusteringXYChart chart) {
		EventQueue.invokeLater(new Runnable() {

            @Override
            public void run() {
            	chart.drawXYChart();
            	JFrame frame = (JFrame) chart;
            	frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            	frame.pack();
            	frame.setLocationRelativeTo(null);
            	frame.setVisible(true);
            }
            
        });
	}
	
	public static void createToggledButtons(final JPanel panel, final XYSeries xySeries, 
			final Set<ClusterPoint2D> points, String displayLabelText, String hideLabelText) {
		// display points
        panel.add(new JButton(new AbstractAction(displayLabelText) {
			private static final long serialVersionUID = 1L;
			@Override
            public void actionPerformed(ActionEvent e) {
				if(xySeries.isEmpty()) {
					for(ClusterPoint2D cp : points) {
						xySeries.add(cp.getPoint().getX(), cp.getPoint().getY());
					}
				}
            }
        }));
        
        // hide points
        panel.add(new JButton(new AbstractAction(hideLabelText) {
			private static final long serialVersionUID = 1L;
			@Override
            public void actionPerformed(ActionEvent e) {
				if(!xySeries.isEmpty()) {
					xySeries.clear();
				}
            }
        }));
	}
	
}
