package com.conlini.es.tmdb.river.pojo;

import org.codehaus.jackson.annotate.JsonProperty;

public class Genre {

	@JsonProperty("id")
	private Integer id;
	
	@JsonProperty("name")
	private String name;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
