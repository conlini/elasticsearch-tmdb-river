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

	private Integer bulkAPIThreshold = 100000;

	public static enum DISCOVERY_TYPE {
		MOVIE("/discover/movie", "movie", Constants.TYPE, Movie.class), TV(
				"/discover/tv", "tv", Constants.TYPE, TV.class), ALL(null,
				null, null, null);

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

	private Map<String, String> filters;

	private int upperYearBound = -1;

	private int lowerYearBound = -1;

	private boolean canTerminate = true;

	@Inject
	protected TMDBRiver(RiverName riverName, RiverSettings settings,
			Client client) {
		super(riverName, settings);
		this.controlFlowManager = new ControlFlowManager(riverName.getName());
		this.controlFlowManager.registerPhaseStageListener(
				PHASE.CONTENT_SCRAPE, PHASE_STAGE.COMPLETE, this);
		this.client = client;
		Map<String, Object> settingMap = settings.settings();
		if (settingMap.containsKey("api_key")) {
			this.apiKey = (String) settingMap.get("api_key");
		}

		if (settingMap.containsKey("discovery_type")) {
			String discovery_type = (String) settingMap.get("discovery_type");
			if (discovery_type.equals("tv")) {
				discoveryType = DISCOVERY_TYPE.TV;
			} else if (discovery_type.equals("movie")) {
				discoveryType = DISCOVERY_TYPE.MOVIE;
			}
		}
		if (settingMap.containsKey("max_pages")) {
			maxPages = (Integer) settingMap.get("max_pages");
		}

		if (settingMap.containsKey("content_mapping")) {
			logger.info("Found user defined mapping");
			Map<String, Object> map = (Map<String, Object>) settingMap
					.get("content_mapping");
			this.mapping = new HashMap<String, Object>();
			this.mapping.put(Constants.TYPE, map);
		}
		if (settingMap.containsKey("bulk_api_threshold")) {
			bulkAPIThreshold = (Integer) settingMap.get("bulk_api_threshold");
		}
		if (settingMap.containsKey("filters")) {
			this.filters = (Map<String, String>) settingMap.get("filters");
		}
		if (settingMap.containsKey("year_range")) {
			String[] range = ((String) settingMap.get("year_range")).split("~");
			this.lowerYearBound = Integer.parseInt(range[0]);
			this.upperYearBound = Integer.parseInt(range[1]);
		}
		// print all the settings that have been extracted. Assert that we
		// Received the api key. Don;t print it out for security reasons.
		logger.info(String.format("Recieved apiKey -->  %s",
				(null != apiKey && !apiKey.equals(""))));
		logger.info(String.format("Discovery Type --> %s", discoveryType));
		logger.info("String max_pages --> " + maxPages);
		logger.info("mapping --> " + mapping);
		logger.info("bulk_api_threshold --> " + bulkAPIThreshold);
		logger.info("Filters -- > " + filters);
		logger.info("Lower/Upper year bounds --> " + this.lowerYearBound + "/"
				+ this.upperYearBound);
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
			if (!client.admin().indices().prepareExists(Constants.INDEX_NAME)
					.get().isExists()) {
				client.admin().indices().prepareCreate(Constants.INDEX_NAME)
						.get();
			}
			// if a user defined mapping has been sent, update the mapping
			if (this.mapping != null) {
				client.admin().indices()
						.preparePutMapping(Constants.INDEX_NAME)
						.setType(Constants.TYPE).setSource(mapping).execute()
						.actionGet();
			}
			RestTemplate template = APIUtil.initTemplate();
			if (this.lowerYearBound != -1 && this.upperYearBound != -1) {
				this.canTerminate = false;
				addYearRange();
			}
			String fetchUrl = buildFetchURL();

			controlFlowManager.startContentScrape(apiKey, discoveryType,
					client, bulkAPIThreshold);

			computeMaxPage(template, fetchUrl);

			this.controlFlowManager.startPageScrape(apiKey, fetchUrl, maxPages);
		} else {
			logger.error("No API Key found. Nothing being pulled");
		}

	}

	private String buildFetchURL() {
		String fetchUrl = Constants.basePath + discoveryType.getPath()
				+ "?api_key={api_key}&page={page_no}";

		if (filters != null && !filters.isEmpty()) {
			fetchUrl = APIUtil.addFilters(fetchUrl, filters);
		}
		logger.info("Fetch URL for discovery --> " + fetchUrl);
		return fetchUrl;
	}

	private void computeMaxPage(RestTemplate template, String fetchUrl) {
		DiscoverResponse response = template.getForObject(fetchUrl,
				DiscoverResponse.class, APIUtil.getVariableVals(apiKey, "1"));
		logger.info(String.format(
				"Received response for %d content. Fetching %d pages ",
				response.getTotalResults(), response.getTotalPages()));
		maxPages = this.maxPages == null ? response.getTotalPages()
				: (this.maxPages < response.getTotalPages() ? this.maxPages
						: response.getTotalPages());
		logger.info("Max page computed --> " + maxPages);
	}

	private void addYearRange() {
		if (this.filters == null) {
			this.filters = new HashMap<String, String>();
		}
		String lte = this.lowerYearBound + "-12-31";
		String gte = this.lowerYearBound + "-01-01";
		this.lowerYearBound++;
		if (discoveryType.equals(DISCOVERY_TYPE.MOVIE)) {
			filters.put("release_date.lte", lte);
			filters.put("release_date.gte", gte);
		} else if (discoveryType.equals(DISCOVERY_TYPE.TV)) {
			filters.put("first_air_date.lte", lte);
			filters.put("first_air_date.gte", gte);
		}

	}

	public void close() {
		logger.info("close called");
		controlFlowManager.close();
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
				&& stage.equals(PHASE_STAGE.COMPLETE) && canTerminate) {
			logger.debug("Done scrapping. Deleting mapping");
			// delete the mapping. We are done with the scrape
			client.admin().indices().prepareDeleteMapping("_river")
					.setType(riverName.name()).execute();

		} else if (phase.equals(PHASE.PAGE_SCRAPE)
				&& stage.equals(PHASE_STAGE.COMPLETE)) {
			if (lowerYearBound > upperYearBound) {
				canTerminate = true;
				logger.info("Fetched complete year range. Can terminate");
			} else {

				addYearRange();
				String fetchUrl = buildFetchURL();
				computeMaxPage(APIUtil.initTemplate(), fetchUrl);
				this.controlFlowManager.startPageScrape(apiKey, fetchUrl,
						maxPages);
			}
		}
	}

}
