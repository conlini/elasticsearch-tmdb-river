package com.conlini.es.tmdb.river.pojo;

import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Credits {

	@JsonProperty("cast")
	private List<Cast> cast;

	public List<Cast> getCast() {
		return cast;
	}

	public void setCast(List<Cast> cast) {
		this.cast = cast;
	}

}
