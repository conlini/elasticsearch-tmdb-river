package com.conlini.es.tmdb.river.core;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class TMDBRiverModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(River.class).to(TMDBRiver.class).asEagerSingleton();
	}


}
