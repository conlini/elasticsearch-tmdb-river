package com.conlini.es.tmdb.river.pojo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TV implements SourceProvider {

	@JsonProperty("name")
	private String title;

	@JsonProperty("genres")
	private List<Genre> genres;

	@JsonProperty("number_of_episodes")
	private Integer numberOfEpisodes;

	@JsonProperty("number_of_seasons")
	private Integer numberOfSeasons;

	@JsonProperty("original_name")
	private String originalTitle;

	@JsonProperty("overview")
	private String overview;

	@JsonProperty("popularity")
	private Double popularity;

	@JsonProperty("vote_average")
	private Double rating;

	@JsonProperty("first_air_date")
	private String releaseDate;

	@JsonProperty("last_air_date")
	private String endDate;

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<Genre> getGenres() {
		return genres;
	}

	public void setGenres(List<Genre> genres) {
		this.genres = genres;
	}

	public Integer getNumberOfEpisodes() {
		return numberOfEpisodes;
	}

	public void setNumberOfEpisodes(Integer numberOfEpisodes) {
		this.numberOfEpisodes = numberOfEpisodes;
	}

	public Integer getNumberOfSeasons() {
		return numberOfSeasons;
	}

	public void setNumberOfSeasons(Integer numberOfSeasons) {
		this.numberOfSeasons = numberOfSeasons;
	}

	public String getOriginalTitle() {
		return originalTitle;
	}

	public void setOriginalTitle(String originalTitle) {
		this.originalTitle = originalTitle;
	}

	public String getOverview() {
		return overview;
	}

	public void setOverview(String overview) {
		this.overview = overview;
	}

	public Double getPopularity() {
		return popularity;
	}

	public void setPopularity(Double popularity) {
		this.popularity = popularity;
	}

	public Double getRating() {
		return rating;
	}

	public void setRating(Double rating) {
		this.rating = rating;
	}

	public String getReleaseDate() {
		return releaseDate;
	}

	public void setReleaseDate(String releaseDate) {
		this.releaseDate = releaseDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	@Override
	public XContentBuilder source() throws IOException, ParseException {
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		builder.field("originalTitle", originalTitle);
		builder.field("overview", overview);
		builder.field("title", title);
		builder.field("rating", rating);
		builder.field("popularity", popularity);
		builder.field("number_of_seasons", numberOfSeasons);
		builder.field("number_of_episodes", numberOfEpisodes);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");

		if (null != releaseDate && !releaseDate.isEmpty()) {
			builder.field("releaseDate", sdf.parse(releaseDate));
		}

		if (null != endDate && !endDate.isEmpty()) {
			builder.field("endDate", sdf.parse(endDate));
		}
		if (null != genres && !genres.isEmpty()) {
			builder.field("genres").startArray();
			for (Genre genre : genres) {
				builder.startObject();
				builder.field("id", genre.getId());
				builder.field("name", genre.getName());
				builder.endObject();
			}
			builder.endArray();

		}
		builder.endObject();
		return builder;
	}

}
