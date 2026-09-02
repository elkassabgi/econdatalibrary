# econdatalibrary - R client for the Econ Data Library (econdatalibrary.com)
#
# Quick start:
#   library(econdatalibrary)
#   edl_set_key("YOUR_API_KEY")               # or set env var EDL_API_KEY
#   src  <- edl_sources()                     # every source we actually serve
#   hits <- edl_search("unemployment rate")   # find series in the catalogue
#   df   <- edl_series("bls:LNS14000000")     # observations -> data.frame
#   meta <- edl_metadata("bls:LNS14000000")   # licence, attribution, citation
#   panel <- edl_fetch("worldbank", "NY.GDP.MKTP.CD", geo = c("DEU","FRA","ITA"))
#
# Mirrors the Python client (clients/python/econdl) so the two libraries answer the
# same questions with the same words. Every series carries its producer's licence and
# citation: read `meta$citation_long` and cite the PRODUCER first, this library second.
#
# Depends on httr. `edl_series` uses base R's CSV reader, so nothing else is required.

.edl <- new.env(parent = emptyenv())
.edl$base_url <- Sys.getenv("EDL_BASE_URL", "https://econdl-api.elkassabgi.workers.dev")
.edl$api_key  <- Sys.getenv("EDL_API_KEY", "")

.EDL_UA <- "econdatalibrary-r/0.1.0"

#' Set the Econ Data Library API key for this session.
#'
#' Registration is free. Catalogue search and metadata need no key; downloading
#' series data does.
#'
#' @param api_key Your API key.
#' @return Invisibly TRUE.
#' @export
edl_set_key <- function(api_key) {
  stopifnot(is.character(api_key), length(api_key) == 1L, nchar(api_key) > 0)
  .edl$api_key <- api_key
  invisible(TRUE)
}

#' Point the client at a different API base URL (testing, or a local dev server).
#' @param base_url Base URL with no trailing slash.
#' @return Invisibly the previous value.
#' @export
edl_set_base_url <- function(base_url) {
  stopifnot(is.character(base_url), length(base_url) == 1L, nchar(base_url) > 0)
  old <- .edl$base_url
  .edl$base_url <- sub("/+$", "", base_url)
  invisible(old)
}

# Every status this API defines gets its own message, because the whole point of the
# contract is that failures are loud and actionable rather than an empty result:
#   401/403 no or bad key      404 id not in the catalogue
#   451     source is not redistributable -- we hold it, we may not serve it
#   501     source has no resolver yet    502 resolved to a file with zero rows
# 429 and 5xx are retried; the rest are raised immediately, because retrying a 404
# only turns a clear answer into a slow one.
.edl_request <- function(path, query = list(), auth = TRUE,
                         timeout_s = 120, max_retries = 3) {
  if (!requireNamespace("httr", quietly = TRUE)) {
    stop("The 'httr' package is required. install.packages('httr')", call. = FALSE)
  }
  url <- paste0(.edl$base_url, path)
  hdrs <- if (auth && nzchar(.edl$api_key)) {
    httr::add_headers(`User-Agent` = .EDL_UA, `X-API-Key` = .edl$api_key)
  } else {
    httr::add_headers(`User-Agent` = .EDL_UA)
  }
  if (auth && !nzchar(.edl$api_key)) {
    stop("No API key set. Call edl_set_key('...') or set the EDL_API_KEY environment ",
         "variable. Registration is free at https://econdatalibrary.com",
         call. = FALSE)
  }

  last <- NULL
  for (attempt in seq_len(max_retries)) {
    resp <- tryCatch(httr::GET(url, hdrs, query = query, httr::timeout(timeout_s)),
                     error = function(e) { last <<- conditionMessage(e); NULL })
    if (is.null(resp)) { Sys.sleep(1.5 * attempt); next }
    code <- httr::status_code(resp)
    if (code == 200) return(resp)

    detail <- tryCatch({
      body <- httr::content(resp, as = "parsed", type = "application/json")
      if (is.list(body) && !is.null(body$detail)) paste0(" - ", body$detail) else ""
    }, error = function(e) "")

    if (code %in% c(401, 403)) {
      stop("Authentication failed (", code, "). Check your API key.", detail, call. = FALSE)
    }
    if (code == 404) {
      stop("Not found: ", path, ". The id is not in the catalogue.", detail, call. = FALSE)
    }
    if (code == 451) {
      stop("This source is not redistributable, so we cannot serve its data (451).", detail,
           "\nThe catalogue may describe it, but its licence does not permit re-hosting. ",
           "Use the producer's own site - see edl_metadata(id)$homepage.", call. = FALSE)
    }
    if (code == 501) {
      stop("That source has no resolver yet (501).", detail,
           "\nThis is a gap on our side, not a problem with your request.", call. = FALSE)
    }
    if (code == 502) {
      stop("The series resolved to a file with zero rows (502).", detail,
           "\nReported rather than returned as an empty result, deliberately.", call. = FALSE)
    }
    if (code == 429) { Sys.sleep(5 * attempt); next }
    if (code >= 500) { last <- paste("server error", code); Sys.sleep(2 * attempt); next }
    stop("HTTP ", code, " for ", path, detail, call. = FALSE)
  }
  stop("Request to ", path, " failed after ", max_retries, " attempts: ",
       if (is.null(last)) "unknown" else last, call. = FALSE)
}

.edl_json <- function(path, query = list(), auth = TRUE) {
  httr::content(.edl_request(path, query, auth), as = "parsed", type = "application/json")
}

# A list-of-lists from JSON, flattened to a data.frame without dropping rows that are
# missing a field. Absent values become NA rather than shortening the column, which is
# how a naive vapply silently misaligns a table.
.edl_rows_to_df <- function(rows, cols) {
  if (!length(rows)) {
    out <- as.data.frame(matrix(character(0), nrow = 0, ncol = length(cols)),
                         stringsAsFactors = FALSE)
    names(out) <- cols
    return(out)
  }
  # `r[[k]]` THROWS "subscript out of bounds" when k is not a name of the list -- unlike
  # `$`, which returns NULL. A catalogue row legitimately omits fields it has no value
  # for (unit and geography are frequently absent), so the membership test is required,
  # not defensive. Found by running the client against the live API, not by reading it.
  cells <- lapply(cols, function(k) {
    vapply(rows, function(r) {
      v <- if (is.list(r) && k %in% names(r)) r[[k]] else NULL
      if (is.null(v) || length(v) == 0L) NA_character_ else as.character(v)[1]
    }, character(1))
  })
  names(cells) <- cols
  as.data.frame(cells, stringsAsFactors = FALSE)
}

#' Every source the library actually serves.
#'
#' Sources we hold but may not redistribute are deliberately NOT listed: the rule is
#' host it fully or do not list it.
#'
#' @return data.frame with source, name, homepage, license_id, commercial_ok,
#'   status, last_updated, data_through.
#' @export
edl_sources <- function() {
  d <- .edl_json("/v1/sources", auth = FALSE)
  rows <- if (!is.null(d$sources)) d$sources else d
  data.frame(
    source        = vapply(rows, function(r) r$source %||% NA_character_, character(1)),
    name          = vapply(rows, function(r) r$name %||% NA_character_, character(1)),
    homepage      = vapply(rows, function(r) r$homepage %||% NA_character_, character(1)),
    license_id    = vapply(rows, function(r) r$license$id %||% NA_character_, character(1)),
    commercial_ok = vapply(rows, function(r) isTRUE(r$license$commercial_ok), logical(1)),
    status        = vapply(rows, function(r) r$freshness$status %||% NA_character_, character(1)),
    last_updated  = vapply(rows, function(r) r$freshness$last_updated %||% NA_character_, character(1)),
    data_through  = vapply(rows, function(r) r$freshness$data_through %||% NA_character_, character(1)),
    stringsAsFactors = FALSE
  )
}

# Returns `a` UNCHANGED when present. An earlier version returned `a[[1]]`, which is
# harmless for the scalar fields it was written for and silently destructive for a list:
# `d$results %||% list()` collapsed a whole page of catalogue rows to its FIRST row, and
# .edl_rows_to_df then read that row's nine FIELDS as nine ROWS, every value NA. It looked
# like a plausible result -- 9 rows for a limit of 5 -- rather than an error.
`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L) b else a

#' Search the catalogue.
#'
#' @param q      Free-text query (full-text over title and geography). Optional.
#' @param source Restrict to one source id. Optional.
#' @param limit  Rows to return, max 500 per request.
#' @param offset Row offset for paging.
#' @return data.frame of matches, with attribute "total" (all matches) and
#'   "coverage" (the catalogue's own grain caveat).
#' @details Catalogue grain is NOT uniform. Large sources are catalogued per table or
#'   per flow, with every series inside that row's CSV, so a search returning nothing
#'   does not prove a series is unavailable. The API says so in `catalog_coverage`,
#'   which is attached here as the "coverage" attribute.
#' @export
edl_search <- function(q = NULL, source = NULL, limit = 50, offset = 0) {
  query <- list(limit = limit, offset = offset)
  if (!is.null(q))      query$q <- q
  if (!is.null(source)) query$source <- source
  d <- .edl_json("/v1/catalog", query, auth = FALSE)
  out <- .edl_rows_to_df(d$results %||% list(),
                         c("series_id", "source", "title", "frequency", "unit",
                           "geography", "license_id", "start_date", "end_date"))
  attr(out, "total") <- d$total
  attr(out, "coverage") <- d$catalog_coverage
  out
}

#' Metadata for one series: licence, attribution, citation, coverage.
#' @param series_id Exact catalogue id, e.g. "bls:LNS14000000".
#' @return A named list exactly as the API returns it.
#' @export
edl_metadata <- function(series_id) {
  stopifnot(is.character(series_id), length(series_id) == 1L)
  .edl_json(paste0("/v1/series/", utils::URLencode(series_id, reserved = TRUE),
                   ".metadata.json"), auth = FALSE)
}

#' Download one series as a data.frame.
#'
#' @param series_id Exact catalogue id.
#' @param from,to   Optional inclusive date window, "YYYY-MM-DD".
#' @return data.frame with series_id (character), obs_date (Date), value (numeric).
#' @export
edl_series <- function(series_id, from = NULL, to = NULL) {
  stopifnot(is.character(series_id), length(series_id) == 1L)
  query <- list()
  if (!is.null(from)) query$from <- from
  if (!is.null(to))   query$to   <- to
  resp <- .edl_request(paste0("/v1/series/",
                              utils::URLencode(series_id, reserved = TRUE), ".csv"),
                       query)
  txt <- httr::content(resp, as = "text", encoding = "UTF-8")
  # Every .csv carries a '#' citation preamble (since 2026-07-09). A response with NO
  # content-length that is not a gzip passthrough (x-econdl-citation-omitted) was inflated
  # by the server and MUST end with '# econdl-complete rows=N' (CONTRACT.md, 2026-09-02):
  # a server-side abort reaches the client as a clean end of body, so the line is the only
  # proof the transfer was whole (R607).
  hdrs <- httr::headers(resp)
  has_len <- !is.null(hdrs[["content-length"]])
  passthrough <- !is.null(hdrs[["x-econdl-citation-omitted"]])
  # R615: this client does NOT choose its Accept-Encoding. httr's curl handle sets libcurl's
  # CURLOPT_ACCEPT_ENCODING to "" - every encoding the local libcurl build supports (gzip,
  # deflate, and on newer builds br and zstd) - and libcurl decodes the body before R sees a
  # byte. A proxy is therefore free to re-code a gzip passthrough into another encoding, and
  # then content-length describes bytes nobody counted. A passthrough with no content-length
  # is UNVERIFIABLE: it carries no completeness line (the server never inflated it) and no
  # length to check, so nothing proves the transfer was whole.
  if (!has_len && passthrough)
    stop(sprintf(paste0("econdatalibrary: %s: the gzip passthrough arrived without a content-length - ",
                        "an intermediary re-coded the body, so nothing proves the transfer was whole. ",
                        "Retry, or pass from=/to= so the server returns a filtered response that ",
                        "carries the '# econdl-complete rows=N' line"), series_id))
  lines <- strsplit(txt, "\r?\n")[[1]]
  expected <- NA_integer_
  if (!has_len && !passthrough) {
    nonblank <- lines[nzchar(trimws(lines))]
    last <- if (length(nonblank)) nonblank[length(nonblank)] else ""
    m <- regmatches(last, regexec("^#\\s*econdl-complete\\s+rows=([0-9]+)\\s*$", last))[[1]]
    if (length(m) < 2L)
      stop(sprintf("econdatalibrary: %s: the response declared no content-length and does not end with the '# econdl-complete rows=N' line the contract requires - the transfer was cut off; retry", series_id))
    expected <- as.integer(m[2])
  }
  lines <- lines[!grepl("^\\s*#", lines)]
  df <- utils::read.csv(text = paste(lines, collapse = "\n"), stringsAsFactors = FALSE)
  if (!is.na(expected) && nrow(df) != expected)
    stop(sprintf("econdatalibrary: %s: the completeness line says %d rows but %d were parsed - the transfer was cut off; retry", series_id, expected, nrow(df)))
  if ("obs_date" %in% names(df)) df$obs_date <- as.Date(df$obs_date)
  if ("value" %in% names(df))    df$value    <- suppressWarnings(as.numeric(df$value))
  df
}

#' Fetch the same indicator across several geographies as one tidy frame.
#'
#' @param source    Source id, e.g. "worldbank".
#' @param indicator Indicator code, e.g. "NY.GDP.MKTP.CD".
#' @param geo       Character vector of geography codes.
#' @param from,to   Optional inclusive date window.
#' @param quiet     If FALSE (default) a geography that cannot be served is reported
#'   with its reason rather than silently dropped.
#' @return data.frame with geo, series_id, obs_date, value. Geographies that fail are
#'   omitted from the frame and listed in the "failed" attribute.
#' @details Fans out client-side, one request per geography, mirroring the Python
#'   client's `fetch`. A failure for one geography never discards the others, and it
#'   is never silent -- an empty result and a refused request must not look alike.
#' @export
edl_fetch <- function(source, indicator, geo, from = NULL, to = NULL, quiet = FALSE) {
  stopifnot(is.character(source), is.character(indicator), is.character(geo))
  parts <- list()
  failed <- character(0)
  for (g in geo) {
    id <- paste(source, indicator, g, sep = ":")
    df <- tryCatch(edl_series(id, from = from, to = to),
                   error = function(e) {
                     failed[[g]] <<- conditionMessage(e)
                     if (!quiet) message("edl_fetch: ", g, " unavailable - ",
                                         conditionMessage(e))
                     NULL
                   })
    if (!is.null(df) && nrow(df)) {
      df$geo <- g
      parts[[length(parts) + 1L]] <- df
    }
  }
  out <- if (length(parts)) do.call(rbind, parts) else
    data.frame(geo = character(0), series_id = character(0),
               obs_date = as.Date(character(0)), value = numeric(0),
               stringsAsFactors = FALSE)
  if (length(parts)) out <- out[, c("geo", setdiff(names(out), "geo"))]
  attr(out, "failed") <- failed
  out
}

#' Freshness for every source: status, last success, cadence, newest observation.
#' @return data.frame.
#' @export
edl_last_updates <- function() {
  d <- .edl_json("/v1/last-updates", auth = FALSE)
  rows <- if (!is.null(d$sources)) d$sources else if (!is.null(d$results)) d$results else d
  .edl_rows_to_df(rows, c("source_id", "unit_id", "status", "last_success_utc",
                          "upstream_vintage", "last_obs_date", "obs_count", "cadence"))
}

#' Library-wide totals, as measured on the data store.
#' @return A named list. When `recalculating` is TRUE the headline totals are being
#'   revised and `recalculating_note` says so; `catalog_entries` is counted live and
#'   is not affected.
#' @export
edl_stats <- function() {
  # Cache-bust. /v1/stats is edge-cached for six hours (s-maxage=21600), which is right for
  # a figure measured by a periodic census but means a client can otherwise read totals -- and
  # a `recalculating` flag -- that predate the current deployment by most of a day.
  d <- .edl_json("/v1/stats", list(t = as.integer(Sys.time())), auth = FALSE)
  if (isTRUE(d$recalculating)) {
    message("Note: ", d$recalculating_note %||%
              "headline totals are being recalculated and may change.")
  }
  d
}
