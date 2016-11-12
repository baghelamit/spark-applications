package com.abaghel.examples.spark.cassandra;

import java.io.Serializable;
/**
 * User class to represent row in Cassandra
 * 
 * @author abaghel
 *
 */
public class User implements Serializable{
	
    String id;
    String username;
    String email;
    
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
}
