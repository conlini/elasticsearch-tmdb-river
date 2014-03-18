package com.conlini.es.tmdb.river.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJacksonHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

public class APIUtil {

	public static Map<String, ?> getVariableVals(String apiKey, String pageNum) {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("api_key", apiKey);
		values.put("page_no", pageNum);
		return values;
	}

	public static RestTemplate initTemplate() {
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

	public static String addFilters(String fetchUrl, Map<String, String> filters) {
		StringBuilder builder = new StringBuilder(fetchUrl);
		for (Entry<String, String> entry : filters.entrySet()) {
			builder.append("&").append(entry.getKey()).append("=")
					.append(entry.getValue());
		}
		return builder.toString();
	}
}
