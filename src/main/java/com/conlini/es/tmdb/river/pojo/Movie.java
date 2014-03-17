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
public class Movie implements SourceProvider, CreditsOwner {

	@JsonProperty("adult")
	private Boolean adult;

	@JsonProperty("title")
	private String title;

	@JsonProperty("popularity")
	private Double popularity;

	@JsonProperty("overview")
	private String overview;

	@JsonProperty("original_title")
	private String originalTitle;

	@JsonProperty("genres")
	private List<Genre> genres;

	@JsonProperty("release_date")
	private String releaseDate;

	@JsonProperty("vote_average")
	private Double rating;

	@JsonProperty("belongs_to_collection.name")
	private String seriesName;

	private List<String> keywords;

	private Credits credit;

	public Credits getCredit() {
		return credit;
	}

	@Override
	public void setCredit(Credits credit) {
		this.credit = credit;
	}

	public List<String> getKeywords() {
		return keywords;
	}

	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}

	public String getSeriesName() {
		return seriesName;
	}

	public void setSeriesName(String seriesName) {
		this.seriesName = seriesName;
	}

	public Boolean getAdult() {
		return adult;
	}

	public void setAdult(Boolean adult) {
		this.adult = adult;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public Double getPopularity() {
		return popularity;
	}

	public void setPopularity(Double popularity) {
		this.popularity = popularity;
	}

	public String getOverview() {
		return overview;
	}

	public void setOverview(String overview) {
		this.overview = overview;
	}

	public String getOriginalTitle() {
		return originalTitle;
	}

	public void setOriginalTitle(String originalTitle) {
		this.originalTitle = originalTitle;
	}

	public List<Genre> getGenres() {
		return genres;
	}

	public void setGenres(List<Genre> genres) {
		this.genres = genres;
	}

	public String getReleaseDate() {
		return releaseDate;
	}

	public void setReleaseDate(String releaseDate) {
		this.releaseDate = releaseDate;
	}

	public Double getRating() {
		return rating;
	}

	public void setRating(Double rating) {
		this.rating = rating;
	}

	public XContentBuilder source() throws IOException, ParseException {
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		builder.field("adult", adult);
		builder.field("originalTitle", originalTitle);
		builder.field("overview", overview);
		builder.field("title", title);
		builder.field("rating", rating);
		builder.field("popularity", popularity);
		if (null != releaseDate && !releaseDate.isEmpty()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
			builder.field("releaseDate", sdf.parse(releaseDate));
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
		if (null != seriesName) {
			builder.field("series_name", seriesName);
		}
		builder.endObject();
		if (this.keywords != null && !this.keywords.isEmpty()) {
			builder.array("keywords", this.keywords.toArray());
		}
		if (this.credit != null && this.credit.getCast() != null
				&& !this.credit.getCast().isEmpty()) {
			builder.startObject("credits");
			for (Cast cast : this.credit.getCast()) {
				builder.field("cast_id", cast.getId());
				builder.field("character", cast.getCharacter());
				builder.field("person_name", cast.getName());
			}
			builder.endObject();
		}
		return builder;
	}
}
