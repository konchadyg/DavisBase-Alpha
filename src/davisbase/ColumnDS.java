/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package davisbase;

/**
 *
 * @author konchady
 */
public class ColumnDS {
    
	private String colname;
	private String dataType;
	private boolean isPrimaryKey;
	private boolean notnull;

	public ColumnDS() {
	}

	public String getColName() {
		return colname;
	}

	public void setColName(String colname) {
		this.colname = colname;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dt) {
		this.dataType = dt;
	}

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public void setPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}

	public boolean isNotNull() {
		return notnull;
	}

	public void setNotNull(boolean isn) {
		notnull = isn;
	}    
    
}
