#!/usr/bin/env Rscript
# ---------------------------------------------------------------
#  Scrape tweets for a set of handles and upsert into Supabase,
#  plus snapshot follower counts to user_followers.
#  Modes:
#    - SCRAPE_MODE=all       → uses tw_handles.txt (fallback: TW_HANDLES)
#    - SCRAPE_MODE=priority  → uses tw_priority_handles.txt (fallback: TW_PRIORITY_HANDLES)
# ---------------------------------------------------------------

## 0 – packages --------------------------------------------------
need <- c("reticulate", "jsonlite", "purrr", "dplyr",
          "lubridate", "DBI", "RPostgres", "tibble", "stringr")
new  <- need[!need %in% rownames(installed.packages())]
if (length(new)) install.packages(new, repos = "https://cloud.r-project.org")
invisible(lapply(need, library, character.only = TRUE))

## 1 – Python env & twscrape ------------------------------------
venv <- Sys.getenv("PY_VENV_PATH", ".venv")
if (!dir.exists(venv)) {
  reticulate::virtualenv_create(venv, python = NULL)
  reticulate::virtualenv_install(venv, "twscrape")
}
reticulate::use_virtualenv(venv, required = TRUE)

twscrape <- import("twscrape", convert = FALSE)
asyncio  <- import("asyncio",  convert = FALSE)
api      <- twscrape$API()

## 2 – Add account (needs cookies) ------------------------------
cookie_json <- Sys.getenv("TW_COOKIES_JSON")
if (cookie_json == "") stop("TW_COOKIES_JSON env var not set")

cookies_list <- jsonlite::fromJSON(cookie_json)
cookies_str  <- paste(paste0(cookies_list$name, "=", cookies_list$value),
                      collapse = "; ")
asyncio$run(api$pool$add_account("x", "x", "x", "x", cookies = cookies_str))

message(sprintf("✅ %d cookies loaded, total chars = %d",
                nrow(cookies_list), nchar(cookies_str)))

## 3 – Which handles to scrape? ---------------------------------
SCRAPE_MODE <- tolower(Sys.getenv("SCRAPE_MODE", "all"))

read_list_file <- function(path) {
  if (!file.exists(path)) return(character())
  ln <- readLines(path, warn = FALSE, encoding = "UTF-8")
  ln |>
    sub("#.*$", "", x = _) |>
    trimws() |>
    (\(v) unlist(strsplit(v, ",")))() |>
    trimws() |>
    sub("^@", "", x = _) |>
    (\(v) v[nzchar(v)])() |>
    unique()
}

handles_all_file      <- "tw_handles.txt"
handles_priority_file <- "tw_priority_handles.txt"

file_all      <- read_list_file(handles_all_file)
file_priority <- read_list_file(handles_priority_file)

clean_env <- function(x) {
  if (is.null(x) || x == "") return(character())
  unlist(strsplit(x, ",")) |>
    trimws() |>
    sub("^@", "", x = _) |>
    unique()
}

env_all <- Sys.getenv("TW_HANDLES", "aoTheComputer,ar_io_network,samecwilliams")
env_pri <- Sys.getenv("TW_PRIORITY_HANDLES", "HyMatrixOrg,outprog,EverVisionLabs")

handles_all      <- if (length(file_all)) file_all else clean_env(env_all)
handles_priority <- if (length(file_priority)) file_priority else clean_env(env_pri)

handles <- if (SCRAPE_MODE == "priority") handles_priority else handles_all

if (length(handles) == 0) stop("No handles found to scrape (files and env were empty).")
message("🛠  Mode: ", SCRAPE_MODE)
message("✅ Handles source: ", if (SCRAPE_MODE == "priority") {
  if (length(file_priority)) "tw_priority_handles.txt" else "TW_PRIORITY_HANDLES env"
} else {
  if (length(file_all)) "tw_handles.txt" else "TW_HANDLES env"
})
message("✅ Targets: ", paste(handles, collapse = ", "))

# Helper: detect numeric ID vs username
is_numeric_id <- function(x) grepl("^[0-9]{5,}$", x)

## Helpers to safely pull Python attrs --------------------------
py_none <- import_builtins()$None
as_chr  <- function(x) if (!identical(x, py_none)) py_str(x) else NA_character_
as_num  <- function(x) if (!identical(x, py_none)) as.numeric(py_str(x)) else NA_real_

py_attr_first <- function(obj, candidates) {
  for (nm in candidates) {
    if (py_has_attr(obj, nm)) {
      val <- obj[[nm]]
      ch  <- as_chr(val)
      if (!is.na(ch) && nzchar(ch)) return(ch)
    }
  }
  NA_character_
}

# ───────────────────────────────────────────────────────────────
# Canonical tweet URL uses the author's current handle
# ───────────────────────────────────────────────────────────────
tweet_to_list <- function(tw, timeline_owner_handle) {
  author_handle <- py_attr_first(tw$user, c("username", "login", "screen_name"))
  author_id     <- py_attr_first(tw$user, c("id", "rest_id"))

  view <- as_num(tw$viewCount); rep <- as_num(tw$replyCount)
  rt   <- as_num(tw$retweetCount); like <- as_num(tw$likeCount)
  quo  <- as_num(tw$quoteCount);   bok  <- as_num(tw$bookmarkedCount)
  er   <- if (!is.na(view) && view > 0) 100*(rep+rt+like+quo+bok)/view else NA

  id_str  <- py_str(tw$id)
  handle_for_url <- if (!is.na(author_handle) && nzchar(author_handle)) author_handle else timeline_owner_handle
  url_str <- sprintf("https://twitter.com/%s/status/%s", handle_for_url, id_str)

  list(
    username = timeline_owner_handle,
    tweet_id  = id_str,
    tweet_url = url_str,
    user_id   = author_id,
    text      = py_str(tw$rawContent),
    reply_count      = rep,
    retweet_count    = rt,
    like_count       = like,
    quote_count      = quo,
    bookmarked_count = bok,
    view_count       = view,
    date             = py_str(tw$date),
    is_quote         = !is.null(py_to_r(tw$quotedTweet)),
    is_retweet       = !is.null(py_to_r(tw$retweetedTweet)),
    engagement_rate  = er
  )
}

# Empty templates -----------------------------------------------
empty_df <- tibble(
  username=character(), tweet_id=character(), tweet_url=character(),
  user_id=character(), text=character(),
  reply_count=integer(), retweet_count=integer(), like_count=integer(),
  quote_count=integer(), bookmarked_count=integer(), view_count=numeric(),
  date=character(), is_quote=logical(), is_retweet=logical(),
  engagement_rate=numeric()
)

followers_df <- tibble(
  username        = character(),
  user_id         = character(),
  followers_count = numeric(),
  snapshot_time   = as.POSIXct(character())
)

# Optional limit -----------------------------------------------
TWEET_LIMIT <- as.integer(Sys.getenv("TWEET_LIMIT", "100"))

scrape_one <- function(user_or_id, limit = TWEET_LIMIT) {
  tryCatch({
    if (is_numeric_id(user_or_id)) {
      info  <- asyncio$run(api$user_by_id(user_or_id))
      login <- py_attr_first(info, c("username", "login", "screen_name"))
      me_id <- py_attr_first(info, c("id", "rest_id"))
    } else {
      info  <- asyncio$run(api$user_by_login(user_or_id))
      login_from_api <- py_attr_first(info, c("username", "login", "screen_name"))
      login <- if (!is.na(login_from_api) && nzchar(login_from_api)) login_from_api else user_or_id
      me_id <- py_attr_first(info, c("id", "rest_id"))
    }

    if (is.na(me_id) || !nzchar(me_id)) {
      message(sprintf("⚠️  Skipping %s (no user id returned).", user_or_id))
      return(empty_df)
    }

    followers_cnt <- as_num(if (py_has_attr(info, "followersCount")) info$followersCount else py_none)

    followers_df <<- dplyr::bind_rows(
      followers_df,
      tibble(
        username        = as.character(login %||% ""),
        user_id         = as.character(me_id),
        followers_count = followers_cnt,
        snapshot_time   = Sys.time()
      )
    )

    tweets <- asyncio$run(twscrape$gather(api$user_tweets_and_replies(info$id, limit = limit)))
    message(sprintf("✅ %s → %d tweets", login, py_len(tweets)))

    purrr::map_dfr(0:(max(0, py_len(tweets) - 1)),
                   ~ tweet_to_list(tweets$`__getitem__`(.x), login)) |>
      mutate(main_id = me_id)
  }, error = function(e) {
    message(sprintf("❌ %s → %s", user_or_id, conditionMessage(e)))
    empty_df
  })
}

`%||%` <- function(x, y) if (is.null(x) || is.na(x) || x == "") y else x

## Scrape all targets -------------------------------------------
all_tweets <- purrr::map_dfr(handles, scrape_one)
if (nrow(all_tweets) == 0) stop("No tweets scraped — aborting.")

all_tweets <- all_tweets %>%
  distinct(tweet_id, .keep_all = TRUE) %>%
  select(-main_id)

all_tweets <- all_tweets %>%
  mutate(is_rt_text = str_detect(text, "^RT @")) %>%
  arrange(desc(engagement_rate)) %>%
  mutate(
    high_er_flag = (reply_count + retweet_count + like_count +
                    quote_count + bookmarked_count) > view_count,
    suspicious_retweet = engagement_rate > 50 & is_retweet,
    engagement_rate = if_else(high_er_flag | suspicious_retweet, NA_real_, engagement_rate)
  ) %>%
  select(tweet_id, tweet_url, username, user_id, text,
         reply_count, retweet_count, like_count, quote_count,
         bookmarked_count, view_count, date, is_quote, is_retweet, engagement_rate)

## 4 – Supabase connection --------------------------------------
supa_host <- Sys.getenv("SUPABASE_HOST")
supa_user <- Sys.getenv("SUPABASE_USER")
supa_pwd  <- Sys.getenv("SUPABASE_PWD")
if (supa_pwd == "") stop("Supabase password env var not set")

con <- dbConnect(
  Postgres(),
  host = supa_host,
  port = as.integer(Sys.getenv("SUPABASE_PORT", "5432")),
  dbname = Sys.getenv("SUPABASE_DB", "postgres"),
  user = supa_user,
  password = supa_pwd,
  sslmode = "require"
)

# 4a – tweets table --------------------------------------------
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS twitter_raw (
    tweet_id text PRIMARY KEY,
    tweet_url text,
    username text, user_id text, text text,
    reply
