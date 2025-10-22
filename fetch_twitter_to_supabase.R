#!/usr/bin/env Rscript
# ---------------------------------------------------------------
#  Scrape tweets for a set of handles and upsert into Supabase,
#  plus snapshot follower counts to user_followers.
#  Modes:
#    - SCRAPE_MODE=all       → uses TW_HANDLES
#    - SCRAPE_MODE=priority  → uses TW_PRIORITY_HANDLES
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
# Mode: all (default) or priority
SCRAPE_MODE <- tolower(Sys.getenv("SCRAPE_MODE", "all"))

# Comma-separated lists; leading @ are stripped
clean_list <- function(x) {
  if (is.null(x) || x == "") return(character())
  unlist(strsplit(x, ",")) |>
    trimws() |>
    sub("^@", "", x = _) |>
    unique()
}

all_handles_env <- Sys.getenv(
  "TW_HANDLES",
  # put your "all" default list here if you want a fallback:
  "aoTheComputer,ar_io_network,samecwilliams"
)
priority_handles_env <- Sys.getenv(
  "TW_PRIORITY_HANDLES",
  # default to the three priority accounts if not set
  "HyMatrixOrg,outprog,EverVisionLabs"
)

handles <- if (SCRAPE_MODE == "priority") {
  clean_list(priority_handles_env)
} else {
  clean_list(all_handles_env)
}

if (length(handles) == 0) stop("No handles found to scrape.")
message("🛠  Mode: ", SCRAPE_MODE)
message("✅ Handles: ", paste(handles, collapse = ", "))

## Helpers to safely pull Python attrs --------------------------
py_none <- import_builtins()$None
as_chr  <- function(x) if (!identical(x, py_none)) py_str(x) else NA_character_
as_num  <- function(x) if (!identical(x, py_none)) as.numeric(py_str(x)) else NA_real_

tweet_to_list <- function(tw, user) {
  view <- as_num(tw$viewCount); rep <- as_num(tw$replyCount)
  rt   <- as_num(tw$retweetCount); like <- as_num(tw$likeCount)
  quo  <- as_num(tw$quoteCount);   bok  <- as_num(tw$bookmarkedCount)
  er   <- if (!is.na(view) && view > 0) 100*(rep+rt+like+quo+bok)/view else NA
  id_str  <- py_str(tw$id)
  url_str <- sprintf("https://twitter.com/%s/status/%s", user, id_str)

  list(
    username = user,
    tweet_id = id_str,
    tweet_url = url_str,
    user_id  = as_chr(tw$user$id),
    text     = py_str(tw$rawContent),
    reply_count      = rep,
    retweet_count    = rt,
    like_count       = like,
    quote_count      = quo,
    bookmarked_count = bok,
    view_count       = view,
    date             = py_str(tw$date),
    is_quote   = !is.null(py_to_r(tw$quotedTweet)),
    is_retweet = !is.null(py_to_r(tw$retweetedTweet)),
    engagement_rate  = er
  )
}

# empty tibble templates ----------------------------------------
empty_df <- tibble::tibble(
  username=character(), tweet_id=character(), tweet_url=character(),
  user_id=character(), text=character(),
  reply_count=integer(), retweet_count=integer(), like_count=integer(),
  quote_count=integer(), bookmarked_count=integer(), view_count=numeric(),
  date=character(), is_quote=logical(), is_retweet=logical(),
  engagement_rate=numeric()
)

followers_df <- tibble::tibble(
  username        = character(),
  user_id         = character(),
  followers_count = numeric(),
  snapshot_time   = as.POSIXct(character())
)

# per-user scrape ------------------------------------------------
# Optional: limit via env TWEET_LIMIT (default 100)
TWEET_LIMIT <- as.integer(Sys.getenv("TWEET_LIMIT", "100"))

scrape_one <- function(user, limit = TWEET_LIMIT) {
  tryCatch({
    info  <- asyncio$run(api$user_by_login(user))
    me_id <- as_chr(info$id)

    followers_df <<- dplyr::bind_rows(
      followers_df,
      tibble(
        username        = user,
        user_id         = me_id,
        followers_count = as_num(info$followersCount),
        snapshot_time   = Sys.time()
      )
    )

    tweets <- asyncio$run(
      twscrape$gather(api$user_tweets_and_replies(info$id, limit = limit))
    )
    message(sprintf("✅ %s → %d tweets", user, py_len(tweets)))

    purrr::map_dfr(
      0:(max(0, py_len(tweets) - 1)),
      ~ tweet_to_list(tweets$`__getitem__`(.x), user)
    ) |>
    mutate(main_id = me_id)
  }, error = function(e) {
    message(sprintf("❌ %s → %s", user, conditionMessage(e)))
    empty_df
  })
}

all_tweets <- purrr::map_dfr(handles, scrape_one)
if (nrow(all_tweets) == 0) stop("No tweets scraped — aborting.")

# de-dup & light hygiene ----------------------------------------
all_tweets <- all_tweets %>% distinct(tweet_id, .keep_all = TRUE) %>% select(-main_id)

# classify & guard ER outliers ----------------------------------
all_tweets <- all_tweets %>%
  mutate(is_rt_text = str_detect(text, "^RT @")) %>%
  arrange(desc(engagement_rate)) %>%
  mutate(
    high_er_flag = (reply_count + retweet_count + like_count +
                    quote_count + bookmarked_count) > view_count,
    suspicious_retweet = engagement_rate > 50 & is_retweet,
    engagement_rate = if_else(
      high_er_flag | suspicious_retweet,
      NA_real_,
      engagement_rate
    )
  ) %>%
  select(
    tweet_id, tweet_url, username, user_id, text,
    reply_count, retweet_count, like_count, quote_count,
    bookmarked_count, view_count, date,
    is_quote, is_retweet, engagement_rate
  )

## 4 – Supabase connection --------------------------------------
supa_host <- Sys.getenv("SUPABASE_HOST")
supa_user <- Sys.getenv("SUPABASE_USER")
supa_pwd  <- Sys.getenv("SUPABASE_PWD")
if (supa_pwd == "") stop("Supabase password env var not set")

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = supa_host,
  port = as.integer(Sys.getenv("SUPABASE_PORT", "5432")),
  dbname = Sys.getenv("SUPABASE_DB", "postgres"),
  user = supa_user,
  password = supa_pwd,
  sslmode = "require"
)

# 4a – tweets table & migration --------------------------------
DBI::dbExecute(con, "
  CREATE TABLE IF NOT EXISTS twitter_raw (
    tweet_id text PRIMARY KEY,
    tweet_url text,
    username text, user_id text, text text,
    reply_count integer, retweet_count integer, like_count integer,
    quote_count integer, bookmarked_count integer, view_count bigint,
    date timestamptz, is_quote boolean, is_retweet boolean,
    engagement_rate numeric
  );
")

DBI::dbExecute(con, "
  ALTER TABLE twitter_raw
  ADD COLUMN IF NOT EXISTS tweet_url text;
")

DBI::dbExecute(con, "
WITH ranked AS (
  SELECT ctid,
         ROW_NUMBER() OVER (
           PARTITION BY tweet_id
           ORDER BY date DESC NULLS LAST
         ) AS rn
  FROM twitter_raw
)
DELETE FROM twitter_raw t
USING ranked r
WHERE t.ctid = r.ctid AND r.rn > 1;
")

DBI::dbExecute(con, "
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM   pg_constraint
    WHERE  conrelid = 'twitter_raw'::regclass
       AND contype = 'p'
  ) THEN
    ALTER TABLE twitter_raw
      ADD CONSTRAINT twitter_raw_pkey PRIMARY KEY (tweet_id);
  END IF;
END$$;
")

DBI::dbWriteTable(con, "tmp_twitter_raw", all_tweets,
                  temporary = TRUE, overwrite = TRUE)

DBI::dbExecute(con, "
  WITH dedup AS (
    SELECT DISTINCT ON (tweet_id) *
    FROM tmp_twitter_raw
    ORDER BY tweet_id, date DESC
  )
  INSERT INTO twitter_raw AS t
    (tweet_id, tweet_url, username, user_id, text, reply_count,
     retweet_count, like_count, quote_count, bookmarked_count,
     view_count, date, is_quote, is_retweet, engagement_rate)
  SELECT tweet_id, tweet_url, username, user_id, text, reply_count,
         retweet_count, like_count, quote_count, bookmarked_count,
         view_count, date::timestamptz, is_quote, is_retweet, engagement_rate
  FROM dedup
  ON CONFLICT (tweet_id) DO UPDATE SET
    tweet_url        = EXCLUDED.tweet_url,
    reply_count      = EXCLUDED.reply_count,
    retweet_count    = EXCLUDED.retweet_count,
    like_count       = EXCLUDED.like_count,
    quote_count      = EXCLUDED.quote_count,
    bookmarked_count = EXCLUDED.bookmarked_count,
    view_count       = EXCLUDED.view_count,
    engagement_rate  = EXCLUDED.engagement_rate;
")

DBI::dbExecute(con, "DROP TABLE IF EXISTS tmp_twitter_raw;")

# 4b – user_followers -------------------------------------------
DBI::dbExecute(con, "
  CREATE TABLE IF NOT EXISTS user_followers (
    user_id         text,
    username        text,
    followers_count bigint,
    snapshot_time   timestamptz DEFAULT now(),
    PRIMARY KEY (user_id, snapshot_time)
  );
")

DBI::dbWriteTable(con,
                  name      = "user_followers",
                  value     = followers_df,
                  append    = TRUE,
                  row.names = FALSE)

# 5 – wrap up ----------------------------------------------------
DBI::dbDisconnect(con)
message(sprintf("✅ (%s) Tweets & follower counts upserted at %s",
                SCRAPE_MODE, as.character(Sys.time())))

