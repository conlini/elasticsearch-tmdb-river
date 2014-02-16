package com.conlini.es.tmdb.river.core;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

public class TMDBRiverPlugin extends AbstractPlugin {

	@Inject
	public TMDBRiverPlugin() {
	}

	public String description() {
		return "this river pulls data from TMDB and indexes to ES";
	}

	public String name() {
		return "river-tmdb";
	}

	public void onModule(RiversModule module){
		module.registerRiver("tmdb", TMDBRiverModule.class);
	}
}
