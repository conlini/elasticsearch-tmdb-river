package com.conlini.es.tmdb.river.pojo;

import java.io.IOException;
import java.text.ParseException;

import org.elasticsearch.common.xcontent.XContentBuilder;

public interface SourceProvider {

	public abstract XContentBuilder source() throws IOException, ParseException;

}
