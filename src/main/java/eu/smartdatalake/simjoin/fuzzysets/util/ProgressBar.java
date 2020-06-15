package eu.smartdatalake.simjoin.fuzzysets.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//import java.text.DecimalFormat;

public class ProgressBar {
	public int totalSteps;
	public int step;
	public int len;
	public int count;
	// private DecimalFormat etaFormat;
	private static final Logger logger = LogManager.getLogger(ProgressBar.class);

	public ProgressBar(int totalSteps, int len) {
		this.totalSteps = totalSteps;
		this.len = len;
		step = len / totalSteps;
		count = 0;
		// etaFormat = new DecimalFormat("#0.00");
	}

	public ProgressBar(int len) {
		this(20, len);
	}

	public void progress(long time) {

		count++;
		if (len >= totalSteps) {
			if (count % step == 0) {
				long now = System.nanoTime();
				double elapsed = (now - time) / 1000000000.0;
				double eta = (elapsed * step * totalSteps) / count - elapsed;
				/*
				 * System.out.print("|" + StringUtils.repeat("=", count / step)
				 * + StringUtils.repeat(" ", totalSteps - count / step) + "|" +
				 * (count / step * 100) / totalSteps + "% \tElapsed: " + (int)
				 * (elapsed / 60.0) + "m " + (int) (elapsed % 60.0) +
				 * "s \t\tETA: " + (int) (eta / 60.0) + "m " + (int) (eta %
				 * 60.0) + "s\r");
				 */
				String msg = (count / step * 100) / totalSteps + "% \tElapsed: " + (int) (elapsed / 60.0) + "m "
						+ (int) (elapsed % 60.0) + "s \t\tETA: " + (int) (eta / 60.0) + "m " + (int) (eta % 60.0)
						+ "s\r";
//				logger.info(msg);
				System.out.print(msg);
			}
		}
	}
}