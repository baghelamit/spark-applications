package com.abaghel.examples.spark.cassandra;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import scala.Tuple4;
/**
 * UserData class to represent row in cassandra
 * 
 * @author abaghel
 *
 */
public class UserData implements Serializable{
	
    UUID id;
    String username;
    String email;
    List<Tuple4<String, String, String, String>> attributes;
    
	public UUID getId() {
		return id;
	}
	public void setId(UUID id) {
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
	public List<Tuple4<String, String, String, String>> getAttributes() {
		return attributes;
	}
	public void setAttributes(List<Tuple4<String, String, String, String>> attributes) {
		this.attributes = attributes;
	}
    
}
