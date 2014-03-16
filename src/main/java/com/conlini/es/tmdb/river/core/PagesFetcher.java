package com.conlini.es.tmdb.river.core;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.springframework.web.client.RestTemplate;

import com.conlini.es.tmdb.river.core.ControlFlowManager.PHASE;
import com.conlini.es.tmdb.river.core.ControlFlowManager.PHASE_STAGE;
import com.conlini.es.tmdb.river.pojo.DiscoverResponse;

public class PagesFetcher implements Runnable {

	private final ESLogger logger;
	private Integer totalPages;

	private String fetchUrl;

	private RestTemplate template;

	private String apiKey;
	private ControlFlowManager controlFlowManager;

	public PagesFetcher(String riverName, String apiKey, Integer totalPages,
			String fetchUrl, RestTemplate template,
			ControlFlowManager controlFlowManager) {
		super();
		this.totalPages = totalPages;
		this.fetchUrl = fetchUrl;
		this.template = template;
		this.apiKey = apiKey;
		this.controlFlowManager = controlFlowManager;
		this.logger = Loggers.getLogger(getClass(), riverName);
	}

	@Override
	public void run() {
		for (int i = 1; i <= totalPages; i++) {
			logger.info("Fetching page no - " + i);
			DiscoverResponse response = template.getForObject(fetchUrl,
					DiscoverResponse.class,
					APIUtil.getVariableVals(apiKey, i + ""));
			try {
				controlFlowManager.getPageResultQueue().offer(
						response.getResults(), 1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error("Failed to offer results to the queue", e);
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("Error", e);
			}
		}
		logger.info("Done fetching all pages. Signalling end of phase");
		controlFlowManager.notifyPhase(PHASE.PAGE_SCRAPE, PHASE_STAGE.COMPLETE);
	}

}
