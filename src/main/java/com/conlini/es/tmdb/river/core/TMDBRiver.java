package com.conlini.es.tmdb.river.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.springframework.web.client.RestTemplate;

import com.conlini.es.tmdb.river.core.ControlFlowManager.PHASE;
import com.conlini.es.tmdb.river.core.ControlFlowManager.PHASE_STAGE;
import com.conlini.es.tmdb.river.core.ControlFlowManager.PhaseStageListener;
import com.conlini.es.tmdb.river.pojo.DiscoverResponse;
import com.conlini.es.tmdb.river.pojo.DiscoverResult;
import com.conlini.es.tmdb.river.pojo.Movie;
import com.conlini.es.tmdb.river.pojo.SourceProvider;
import com.conlini.es.tmdb.river.pojo.TV;

public class TMDBRiver extends AbstractRiverComponent implements River,
		PhaseStageListener {

	private Client client;

	private String apiKey;

	private Integer maxPages;

	private boolean lastPageFetched = false;

	private Map<String, Object> mapping;

	public static enum DISCOVERY_TYPE {
		MOVIE("/discover/movie", "movie", "contents", Movie.class), TV(
				"/discover/tv", "tv", "contents", TV.class), ALL(null, null,
				null, null);

		public final String path;

		public final String contentPath;

		public final String esType;

		public final Class<? extends SourceProvider> sourceClass;

		private DISCOVERY_TYPE(String path, String contentPath, String esType,
				Class<? extends SourceProvider> sourceClass) {
			this.path = path;
			this.esType = esType;
			this.contentPath = contentPath;
			this.sourceClass = sourceClass;
		}

		public String getPath() {
			return this.path;
		}

		public String getEsType() {
			return this.esType;
		}

		public String getContentPath() {
			return this.contentPath;
		}

	}

	private DISCOVERY_TYPE discoveryType = DISCOVERY_TYPE.MOVIE;

	private ControlFlowManager controlFlowManager;

	private BlockingQueue<List<DiscoverResult>> queues = new ArrayBlockingQueue<List<DiscoverResult>>(
			1);

	@Inject
	protected TMDBRiver(RiverName riverName, RiverSettings settings,
			Client client) {
		super(riverName, settings);
		this.controlFlowManager = new ControlFlowManager(riverName.getName());
		this.controlFlowManager.registerPhaseStageListener(
				PHASE.CONTENT_SCRAPE, PHASE_STAGE.COMPLETE, this);
		this.client = client;
		if (settings.settings().containsKey("api_key")) {
			this.apiKey = (String) settings.settings().get("api_key");
		}

		if (settings.settings().containsKey("discovery_type")) {
			String discovery_type = (String) settings.settings().get(
					"discovery_type");
			if (discovery_type.equals("tv")) {
				discoveryType = DISCOVERY_TYPE.TV;
			} else if (discovery_type.equals("movie")) {
				discoveryType = DISCOVERY_TYPE.MOVIE;
			}
		}
		if (settings.settings().containsKey("max_pages")) {
			maxPages = (Integer) settings.settings().get("max_pages");
		}

		if (settings.settings().containsKey("content_mapping")) {
			logger.info("Found user defined mapping");
			Map<String, Object> map = (Map<String, Object>) settings.settings()
					.get("content_mapping");
			this.mapping = new HashMap<String, Object>();
			this.mapping.put("content", map);
		}
		// print all the settings that have been extracted. Assert that we
		// Received the api key. Don;t print it out for security reasons.
		logger.info(String.format("Recieved apiKey -  %s",
				(null != apiKey && !apiKey.equals(""))));
		logger.info(String.format("Discovery Type = %s", discoveryType));
		logger.info("String max_pages - " + maxPages);
		logger.info("mapping", mapping);
	}

	public RiverName riverName() {
		return this.riverName;
	}

	public void start() {
		logger.info(String.format("Starting %s river", riverName));
		// check if the apiKey has been signalled. There is no point of
		// proceeding if that is not there
		if (null != apiKey && !apiKey.equals("")) {
			// intitalize the index
			if (!client.admin().indices().prepareExists("tmdb").get()
					.isExists()) {
				client.admin().indices().prepareCreate("tmdb").get();
			}
			// if a user defined mapping has been sent, update the mapping
			if (this.mapping != null) {
				client.admin().indices().preparePutMapping("tmdb")
						.setType("content").setSource(mapping).execute()
						.actionGet();
			}
			RestTemplate template = APIUtil.initTemplate();
			String fetchUrl = Constants.basePath + discoveryType.getPath()
					+ "?api_key={api_key}&page={page_no}";
			// Hit the first request. We can then get the total pages to be
			// fetched. Do this only if the maxPages is not provided.
			if (null == maxPages) {
				DiscoverResponse response = template.getForObject(fetchUrl,
						DiscoverResponse.class,
						APIUtil.getVariableVals(apiKey, "1"));
				// Start 1 thread to get the remaining pages. add to a queue
				// start a thread that gets the Content
				logger.info(String.format(
						"Received response for %d content. Fetching %d pages ",
						response.getTotalResults(), response.getTotalPages()));
				maxPages = response.getTotalPages();
			}
			controlFlowManager
					.startContentScrape(apiKey, discoveryType, client);
			this.controlFlowManager.startPageScrape(apiKey, fetchUrl, maxPages);
		} else {
			logger.error("No API Key found. Nothing being pulled");
		}

	}

	public void close() {
	}

	public Client getClient() {
		return client;
	}

	public void setClient(Client client) {
		this.client = client;
	}

	@Override
	public void onPhase(PHASE phase, PHASE_STAGE stage) {
		if (phase.equals(PHASE.CONTENT_SCRAPE)
				&& stage.equals(PHASE_STAGE.COMPLETE)) {
			logger.debug("Done scrapping. Deleting mapping");
			// delete the mapping. We are done with the scrape
			client.admin().indices().prepareDeleteMapping("_river")
					.setType(riverName.name()).execute();

		}
	}

}
