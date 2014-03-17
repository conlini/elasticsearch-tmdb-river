package com.conlini.es.tmdb.river.pojo;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Keyword {

	private List<String> keyWords = null;

	public List<String> getKeyWords() {
		if (null == this.keyWords) {
			keyWords = new ArrayList<String>(innerKeywords.size());
			for (InnerKeyword innerKeyword : innerKeywords) {
				keyWords.add(innerKeyword.getKeyword());
			}
		}
		return keyWords;

	}

	@JsonProperty("keywords")
	private List<InnerKeyword> innerKeywords;

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class InnerKeyword {

		@JsonProperty("name")
		private String keyword;

		public String getKeyword() {
			return keyword;
		}

		public void setKeyword(String keyword) {
			this.keyword = keyword;
		}

	}
}
