package de.julielab.xmlData.cli;

public class QueryOptions {
	
	public String queryStr;
	public String fileStr;
	public String tableName;
	public boolean useDelimiter;
	public String xpath;
	public String baseOutDirStr;
	public String batchSizeStr;
	public String limitStr;
	public String whereClause;
	public Integer numberRefHops;
	public boolean pubmedArticleSet;
	public String tableSchema;
	@Override
	public String toString() {
		return "QueryOptions [queryStr=" + queryStr + ", fileStr=" + fileStr + ", tableName=" + tableName
				+ ", useDelimiter=" + useDelimiter + ", xpath=" + xpath + ", baseOutDirStr=" + baseOutDirStr
				+ ", batchSizeStr=" + batchSizeStr + ", limitStr=" + limitStr + ", whereClause=" + whereClause
				+ ", numberRefHops=" + numberRefHops + ", pubmedArticleSet=" + pubmedArticleSet + "]";
	}
	
	
}
