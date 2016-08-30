package com.abaghel.examples.spark.graphframe;
/**
 * Relation class
 * 
 * @author abaghel
 *
 */
public class Relation {

	private String src;
	private String dst;
	private String relationship;
	
	public Relation(){
		
	}

	public Relation(String src, String dst, String relationship) {
		super();
		this.src = src;
		this.dst = dst;
		this.relationship = relationship;
	}

	public String getSrc() {
		return src;
	}

	public void setSrc(String src) {
		this.src = src;
	}

	public String getDst() {
		return dst;
	}

	public void setDst(String dst) {
		this.dst = dst;
	}

	public String getRelationship() {
		return relationship;
	}

	public void setRelationship(String relationship) {
		this.relationship = relationship;
	}

}
