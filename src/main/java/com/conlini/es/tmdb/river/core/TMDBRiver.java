package com.conlini.es.tmdb.river.core;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJacksonHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import com.conlini.es.tmdb.river.pojo.DiscoverResponse;
import com.conlini.es.tmdb.river.pojo.DiscoverResult;
import com.conlini.es.tmdb.river.pojo.Movie;
import com.conlini.es.tmdb.river.pojo.SourceProvider;
import com.conlini.es.tmdb.river.pojo.TV;

public class TMDBRiver extends AbstractRiverComponent implements River {

	private Client client;

	private String apiKey;

	private final String basePath = "http://api.themoviedb.org/3";

	private Integer maxPages;

	private boolean lastPageFetched = false;

	private static enum DISCOVERY_TYPE {
		MOVIE("/discover/movie", "movie", "contents", Movie.class), TV(
				"/discover/tv", "tv", "contents", TV.class);

		private final String path;

		private final String contentPath;

		private final String esType;

		private final Class<? extends SourceProvider> sourceClass;

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

	private BlockingQueue<List<DiscoverResult>> queues = new ArrayBlockingQueue<List<DiscoverResult>>(
			1);

	@Inject
	protected TMDBRiver(RiverName riverName, RiverSettings settings,
			Client client) {
		super(riverName, settings);
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
		// print all the settings that have been extracted. Assert that we
		// Received the api key. Don;t print it out for security reasons.
		logger.info(String.format("Recieved apiKey -  %s",
				(null != apiKey && !apiKey.equals(""))));
		logger.info(String.format("Discovery Type = %s", discoveryType));
		logger.info("String max_pages - " + maxPages);
	}

	public RiverName riverName() {
		return this.riverName;
	}

	public void start() {
		logger.info(String.format("Starting %s river", riverName));
		if (null != apiKey && !apiKey.equals("")) {
			RestTemplate template = initTemplate();
			String fetchUrl = basePath + discoveryType.getPath()
					+ "?api_key={api_key}&page={page_no}";
			DiscoverResponse response = template.getForObject(fetchUrl,
					DiscoverResponse.class, getVariableVals("1"));
			// Start 1 thread to get the remaining pages. add to a queue
			// start a thread that gets the Content
			logger.info(String.format(
					"Received response for %d content. Fetching %d pages ",
					response.getTotalResults(), response.getTotalPages()));
			ExecutorService service = Executors.newCachedThreadPool();
			ContentFetcher contentFetcher = new ContentFetcher(template);
			Future<Object> future = service.submit(contentFetcher);
			if (null == maxPages) {
				maxPages = response.getTotalPages();
			}
			PagesFetcher pagesFetcher = new PagesFetcher(maxPages, fetchUrl,
					template);
			service.submit(pagesFetcher);
			try {
				Object complete = future.get();
			} catch (InterruptedException e) {
				logger.error("Error", e);
			} catch (ExecutionException e) {
				logger.error("Error", e);
			}
		} else {
			logger.error("No API Key found. Nothing being pulled");
		}
		client.admin().indices().prepareDeleteMapping("_river")
				.setType(riverName.name()).execute();

	}

	private Map<String, ?> getVariableVals(String pageNum) {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("api_key", apiKey);
		values.put("page_no", pageNum);
		return values;
	}

	private RestTemplate initTemplate() {
		RestTemplate template = new RestTemplate();
		List<HttpMessageConverter<?>> convertors = template
				.getMessageConverters();
		MappingJacksonHttpMessageConverter converter = new MappingJacksonHttpMessageConverter();
		List<MediaType> mediaTypes = new ArrayList<MediaType>();
		mediaTypes.add(new MediaType("application", "json"));
		converter.setSupportedMediaTypes(mediaTypes);
		convertors.add(converter);
		template.setMessageConverters(convertors);
		return template;
	}

	public void close() {
	}

	public Client getClient() {
		return client;
	}

	public void setClient(Client client) {
		this.client = client;
	}

	private class PagesFetcher implements Callable<Object> {

		private Integer totalPages;

		private String fetchUrl;

		private RestTemplate template;

		public PagesFetcher(Integer totalPages, String fetchUrl,
				RestTemplate template) {
			super();
			this.totalPages = totalPages;
			this.fetchUrl = fetchUrl;
			this.template = template;
		}

		@Override
		public Object call() throws Exception {
			for (int i = 1; i <= totalPages; i++) {
				logger.info("Fetching page no - " + i);
				DiscoverResponse response = template.getForObject(fetchUrl,
						DiscoverResponse.class, getVariableVals(i + ""));
				try {
					queues.offer(response.getResults(), 2, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					logger.error("Failed to offer results to the queue", e);
				}
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					logger.error("Error", e);
				}
			}
			lastPageFetched = true;
			return new Object();
		}
	}

	private class ContentFetcher implements Callable<Object> {

		private boolean running = true;

		private final String fetchUrl = basePath
				+ "/{type}/{id}?api_key={api_key}";

		private RestTemplate template;

		public ContentFetcher(RestTemplate template) {
			super();
			this.template = template;
		}

		@SuppressWarnings("unchecked")
		private void fetchContents(List<DiscoverResult> results) {
			BulkRequestBuilder requestBuilder = client.prepareBulk();
			logger.info(String.format("Fetching movies - %s", results));
			for (DiscoverResult result : results) {
				SourceProvider sourceProvider = template.getForObject(fetchUrl,
						discoveryType.sourceClass, discoveryType
								.getContentPath(), result.getId().toString(),
						apiKey);

				try {
					requestBuilder.add(client.prepareIndex("tmdb",
							discoveryType.getEsType(),
							result.getId().toString()).setSource(
							sourceProvider.source()));
				} catch (IOException e) {
					logger.error("Error", e);
				} catch (ParseException e) {
					logger.error("Error", e);
				}
			}
			requestBuilder.execute().actionGet();
		}

		@Override
		public Object call() throws Exception {
			while (running) {
				try {
					List<DiscoverResult> results = queues.take();
					fetchContents(results);
					if (lastPageFetched && queues.isEmpty()) {
						// a very dirty way to end the fetch of data. We do this
						// as we need to now unregister the river for future
						// auto fetches.
						// FIXME need a cleaner sync between threads to do this
						running = false;
					}
				} catch (InterruptedException e) {
					logger.error("Failed to take next from queue", e);
					running = false;
				}
			}
			return new Object();
		}
	}
}
