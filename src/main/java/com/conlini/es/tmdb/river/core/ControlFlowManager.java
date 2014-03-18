package com.conlini.es.tmdb.river.core;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.elasticsearch.client.Client;

import com.conlini.es.tmdb.river.core.TMDBRiver.DISCOVERY_TYPE;
import com.conlini.es.tmdb.river.pojo.DiscoverResult;

public class ControlFlowManager {

	public static enum PHASE {
		CONTENT_SCRAPE, PAGE_SCRAPE
	}

	public static enum PHASE_STAGE {
		IN_PROGRESS, COMPLETE
	}

	public static interface PhaseStageListener {

		void onPhase(PHASE phase, PHASE_STAGE stage);
	}

	private static class QueueManager {

		private BlockingQueue<List<DiscoverResult>> pageResults = new ArrayBlockingQueue<List<DiscoverResult>>(
				1);

	}

	private QueueManager queueManager;
	private String riverName;
	private ExecutorService executorService;
	private Future<Boolean> contentCall;
	private List<PhaseStageListener> liseners;

	public ControlFlowManager(String riverName) {
		this.queueManager = new QueueManager();
		this.riverName = riverName;
		this.executorService = Executors.newCachedThreadPool();
		this.liseners = new LinkedList<ControlFlowManager.PhaseStageListener>();
	}

	public BlockingQueue<List<DiscoverResult>> getPageResultQueue() {
		return this.queueManager.pageResults;
	}

	public void startPageScrape(String apiKey, String endPoint,
			Integer totalPages) {
		PagesFetcher fetcher = new PagesFetcher(riverName, apiKey, totalPages,
				endPoint, APIUtil.initTemplate(), this);
		executorService.submit(fetcher);

	}

	public void startContentScrape(String apiKey, DISCOVERY_TYPE discoveryType,
			Client client, Integer bulkAPIThreshold) {
		ContentFetcher fetcher = new ContentFetcher(riverName, discoveryType,
				client, apiKey, this, bulkAPIThreshold);
		executorService.submit(fetcher);
	}

	public void registerPhaseStageListener(PHASE phase, PHASE_STAGE stage,
			PhaseStageListener listener) {
		this.liseners.add(listener);
	}

	public void notifyPhase(PHASE phase, PHASE_STAGE stage) {
		for (PhaseStageListener listener : liseners) {
			listener.onPhase(phase, stage);
		}
	}

	public void close() {
		this.liseners.clear();
		executorService.shutdownNow();
	}
}
