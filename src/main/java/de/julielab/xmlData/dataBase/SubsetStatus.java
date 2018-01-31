package de.julielab.xmlData.dataBase;

import java.text.DecimalFormat;
import java.util.Map.Entry;
import java.util.SortedMap;

public class SubsetStatus {
	private static final DecimalFormat df = new DecimalFormat("#,###,###,##0.00");

	public long total;
	public long inProcess;
	public long isProcessed;
	public long hasErrors;
	public SortedMap<String, Long> pipelineStates;

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("have errors: ");
		sb.append(hasErrors);
		sb.append(", in process: ");
		sb.append(inProcess);
		sb.append(", is processed: ");
		sb.append(isProcessed);
		sb.append(", untouched: ");
		sb.append(String.valueOf(total - inProcess - isProcessed));
		sb.append(", total: ");
		sb.append(total);
		sb.append("\n");

		sb.append("   ");
		sb.append(String.valueOf(df.format(((double) inProcess / total) * 100)));
		sb.append("% in process\n");

		sb.append("   ");
		sb.append(String.valueOf(df.format(((double) isProcessed / total) * 100)));
		sb.append("% is processed\n");
		
		if (pipelineStates != null) {
			sb.append("Pipeline states:\n");
			for (Entry<String, Long> entry : pipelineStates.entrySet()) {
				sb.append("  ");
				sb.append(entry.getKey());
				sb.append(":  ");
				sb.append(entry.getValue());
				sb.append("\n");
			}
		}
		return sb.toString();
	}
}
