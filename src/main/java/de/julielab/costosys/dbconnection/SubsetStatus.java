package de.julielab.costosys.dbconnection;

import java.text.DecimalFormat;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.StringJoiner;

public class SubsetStatus {
    private static final DecimalFormat df = new DecimalFormat("#,###,###,##0.00");

    public Long total;
    public Long inProcess;
    public Long isProcessed;
    public Long hasErrors;
    public SortedMap<String, Long> pipelineStates;

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(", ");
        if (hasErrors != null)
            sj.add("have errors: " + hasErrors);
        if (inProcess != null)
            sj.add("in process: " + inProcess);
        if (isProcessed != null)
            sj.add("is processed: " + isProcessed);
        if (total != null && inProcess != null && isProcessed != null)
            sj.add("untouched: " + String.valueOf(total - inProcess - isProcessed));
        if (total != null)
            sj.add("total: " + total);
        StringBuilder sb = new StringBuilder();
        sb.append(sj.toString());
        sb.append("\n");

        if (inProcess != null && total != null) {
            sb.append("   ");
            sb.append(String.valueOf(df.format(((double) inProcess / total) * 100)));
            sb.append("% in process\n");
        }

        if (isProcessed != null && total != null) {
            sb.append("   ");
            sb.append(String.valueOf(df.format(((double) isProcessed / total) * 100)));
            sb.append("% is processed\n");
        }

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
