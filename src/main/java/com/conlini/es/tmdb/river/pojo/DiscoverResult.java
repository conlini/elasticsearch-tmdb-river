package com.conlini.es.tmdb.river.pojo;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DiscoverResult {

	@JsonProperty("id")
	private Integer id;

	@Override
	public String toString() {
		return "DiscoverResults [id=" + id + "]";
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	
}
