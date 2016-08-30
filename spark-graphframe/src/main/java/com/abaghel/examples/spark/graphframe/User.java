package com.abaghel.examples.spark.graphframe;
/**
 * User class
 * 
 * @author abaghel
 *
 */
public class User {
	private String id;
	private String name;
	private int age;
	
	public User(){		
	}
	
	public User(String id, String name, int age) {
		super();
		this.id = id;
		this.name = name;
		this.age = age;
	}

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
}
