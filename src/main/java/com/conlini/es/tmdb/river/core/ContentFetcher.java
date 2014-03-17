package com.conlini.es.tmdb.river.core;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.springframework.web.client.RestTemplate;

import com.conlini.es.tmdb.river.core.ControlFlowManager.PHASE;
import com.conlini.es.tmdb.river.core.ControlFlowManager.PHASE_STAGE;
import com.conlini.es.tmdb.river.core.ControlFlowManager.PhaseStageListener;
import com.conlini.es.tmdb.river.core.TMDBRiver.DISCOVERY_TYPE;
import com.conlini.es.tmdb.river.pojo.Credits;
import com.conlini.es.tmdb.river.pojo.CreditsOwner;
import com.conlini.es.tmdb.river.pojo.DiscoverResult;
import com.conlini.es.tmdb.river.pojo.Keyword;
import com.conlini.es.tmdb.river.pojo.Movie;
import com.conlini.es.tmdb.river.pojo.SourceProvider;

public class ContentFetcher implements Runnable, PhaseStageListener {

	private final String fetchUrl = Constants.basePath
			+ "/{type}/{id}?api_key={api_key}";

	private final String additionalDataFetchUrl = Constants.basePath
			+ "/{type}/{id}/{data_type}?api_key={api_key}";

	private DISCOVERY_TYPE discoveryType;

	private RestTemplate template = APIUtil.initTemplate();

	private Client client;

	private ESLogger logger;

	private String apiKey;

	private boolean running = true;

	private ControlFlowManager controlFlowManager;

	private boolean can_stop = false;;

	@SuppressWarnings("unchecked")
	private void fetchContents(List<DiscoverResult> results,
			BulkRequestBuilder requestBuilder) {
		logger.info(String.format("Fetching %s - %s",
				discoveryType.contentPath, results));
		for (DiscoverResult result : results) {
			logger.info(String.format("Fetching %s with id %d",
					discoveryType.contentPath, result.getId()));
			SourceProvider sourceProvider = template.getForObject(fetchUrl,
					discoveryType.sourceClass, discoveryType.getContentPath(),
					result.getId().toString(), apiKey);
			logger.info(String.format("Fetched %s with id %d",
					discoveryType.contentPath, result.getId()));
			Credits credits = template.getForObject(additionalDataFetchUrl,
					Credits.class, discoveryType.getContentPath(), result
							.getId().toString(), "credits", apiKey);

			logger.info(String.format("Fetched credit for %s with id %d",
					discoveryType.contentPath, result.getId()));
			((CreditsOwner) sourceProvider).setCredit(credits);

			if (discoveryType.equals(DISCOVERY_TYPE.MOVIE)) {
				logger.info("Fetching keywords");
				Keyword keyword = template.getForObject(additionalDataFetchUrl,
						Keyword.class, discoveryType.getContentPath(), result
								.getId().toString(), "keywords", apiKey);
				System.out.println("ContentFetcher.fetchContents() --> "
						+ keyword.getKeyWords());
				((Movie) sourceProvider).setKeywords(keyword.getKeyWords());

			}
			try {
				logger.info("Adding to BulkAPI");
				requestBuilder.add(client.prepareIndex(
						"tmdb",
						discoveryType.getEsType(),
						discoveryType.getContentPath().toLowerCase() + "_"
								+ result.getId().toString()).setSource(
						sourceProvider.source()));
			} catch (IOException e) {
				logger.error("Error", e);
			} catch (ParseException e) {
				logger.error("Error", e);
			}
		}
	}

	@Override
	public void run() {
		BulkRequestBuilder requestBuilder = client.prepareBulk();
		while (running) {
			try {
				List<DiscoverResult> results = controlFlowManager
						.getPageResultQueue().take();

				fetchContents(results, requestBuilder);
				if (requestBuilder.numberOfActions() > 100000) {
					logger.info("Bulk request threshold reached. Indexing data");
					requestBuilder.get();
					requestBuilder = client.prepareBulk();
				}
				// check that we are done with all pages
				if (controlFlowManager.getPageResultQueue().isEmpty()
						&& can_stop) {
					running = false;
				}

			} catch (InterruptedException e) {
				logger.error("Failed to take next from queue", e);
				running = false;
			}
		}
		// done scrape/ Flush any documents that are queues for indexing
		if (requestBuilder.numberOfActions() > 0) {
			requestBuilder.get();
		}
		logger.info("Done scrapping all contents. Signalling complete phase");
		controlFlowManager.notifyPhase(PHASE.CONTENT_SCRAPE,
				PHASE_STAGE.COMPLETE);
	}

	public ContentFetcher(String riverName, DISCOVERY_TYPE discoveryType,
			Client client, String apiKey, ControlFlowManager controlFlowManager) {
		super();
		this.discoveryType = discoveryType;
		this.client = client;
		this.apiKey = apiKey;
		this.controlFlowManager = controlFlowManager;
		this.logger = Loggers.getLogger(getClass(), riverName);
		this.controlFlowManager.registerPhaseStageListener(PHASE.PAGE_SCRAPE,
				PHASE_STAGE.COMPLETE, this);
	}

	@Override
	public void onPhase(PHASE phase, PHASE_STAGE stage) {
		if (phase.equals(PHASE.PAGE_SCRAPE)
				&& stage.equals(PHASE_STAGE.COMPLETE)) {
			logger.info("Recieved complete page scrape signal");
			this.can_stop = true;
			// this is the case where we page scrape and content scrape finish
			// at the same time
			if (controlFlowManager.getPageResultQueue().isEmpty()) {
				logger.info("Stopping content scrape. All pages are fetched");
				running = false;
			}
		}
	}

}
