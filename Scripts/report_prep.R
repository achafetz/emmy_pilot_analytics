# PROJECT:  emmy_pilot_analytics
# PURPOSE:  Ongoing report pre-processing
# AUTHOR:   A.Chafetz | CMS
# REF ID:   cdb07836a538
# LICENSE:  MIT
# DATE:     2026-04-10
# UPDATED:

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(glue)
library(emmytics) #remotes::install_github("achafetz/emmytics")
library(scales, warn.conflicts = FALSE)
library(systemfonts)
library(tidytext)
library(patchwork)
library(ggtext)
library(arrow)


# GLOBAL VARIABLES --------------------------------------------------------

ref_id <- "cdb07836a538" #a reference to be placed in viz captions

#State agency abbr for API
agency <- "la_ldh"

#report type
report_type <- "month" #week

#on-going pilot start date
pilot_start <- pilot_pds |>
  filter(client_agency == agency) |>
  slice_tail(n = 1) |>
  pull(start_date)

#lookback start
lookback_start <- max(c(pilot_start, today() - months(1)))

#lookback end
lookback_end <- today()

#local path
path <- str_glue(
  "Data/mixpanel_data_{agency}_{lookback_start}_to_{lookback_end}.json"
)

# IMPORT - API -----------------------------------------------------------

if (!file.exists(path)) {
  get_mixpanel_data(
    from_date = lookback_start,
    to_date = lookback_end,
    cache_dir = "Data",
    client_agency = agency
  )
}


# IMPORT - LOCAL ---------------------------------------------------------

df_mp <- read_mixpanel(
  path,
  sync_duration_seconds = properties$sync_duration_seconds,
  flow_started_seconds_ago = properties$flow_started_seconds_ago,
  device_type = properties$device_type,
  employment_type = properties$employment_type,
  employer_name = properties$employment_employer_name,
  query = properties$query,
  drop_prop = TRUE,
  applicant_only = FALSE
)


# MUNGE -------------------------------------------------------------------

#convert to numeric & change from seconds to minutes
df_mp <- df_mp |>
  mutate(
    across(c(sync_duration_seconds, flow_started_seconds_ago), as.numeric),
    flow_started_seconds_ago = flow_started_seconds_ago / 60
  ) |>
  rename(flow_started_minutes_ago = flow_started_seconds_ago)

#establish current and prior comparision reporting periods
df_mp <- df_mp |>
  mutate(
    period = case_when(
      report_type == "month" & timestamp > max(timestamp) - months(1) ~ "curr",
      report_type == "month" & timestamp > max(timestamp) - months(2) ~ "prior",
      report_type == "week" & timestamp > max(timestamp) - weeks(1) ~ "curr",
      report_type == "week" & timestamp > max(timestamp) - weeks(2) ~ "prior",
      TRUE ~ "oob"
    )
  )


# REPORTING PERIOD -------------------------------------------------------

df_pd <- df_mp |>
  summarise(
    date_start = min(as_date(timestamp)),
    date_end = max(as_date(timestamp))
  ) |>
  mutate(
    period_start = max(date_start, date_end = months(1)),
    period = ifelse(
      month(date_start) == month(date_end),
      str_glue(
        '{format.Date(date_start, "%B %d")} - {format.Date(date_end, "%d")}'
      ),
      str_glue(
        '{format.Date(date_start, "%B %d")} - {format.Date(date_end, "%B %d")}'
      )
    )
  )

write_parquet(df_pd, "Dataout/report_period.parquet")

# DROP OFF/JOURNEY -------------------------------------------------------

#dropoff
df_dropoff <- df_mp |>
  filter(
    event %in% key_events,
    period == "curr"
  )

#get a distinct set of events completed for each applicant
df_dropoff <- df_dropoff |>
  distinct(pilot, distinct_id, event)

df_dropoff <- df_dropoff |>
  clean_events() |>
  mutate(event_clean = factor(event_clean, key_events_clean)) |>
  count(event_clean)

df_dropoff <- df_dropoff |>
  mutate(
    n_lag = lag(n),
    loss_share = label_percent(1)(n / n_lag - 1)
  )

df_dropoff <- df_dropoff |>
  mutate(event_clean = fct_recode(event_clean, "Consented" = "Agreed"))

write_parquet(df_dropoff, "Dataout/report_dropoff.parquet")

# ADDITIONAL METRICS -----------------------------------------------------

#subset to current and prior only
df_addtl <- df_mp |>
  filter_out(period == "oob") |>
  filter_out(event == "CaseworkerInvitedApplicantToFlow")

# flag gig/smartphone in order to count distinct
df_addtl <- df_addtl |>
  flag_completed("event") |>
  mutate(
    session_time = case_when(
      event == "ApplicantSharedIncomeSummary" ~ flow_started_minutes_ago
    ),
    completed_with_id = case_when(completed_event == TRUE ~ cbv_flow_id), #distinct_id
    gig_with_id = case_when(employment_type == "gig" ~ cbv_flow_id), #distinct_id
    smartphone_with_id = case_when(device_type == "smartphone" ~ cbv_flow_id) #distinct_id
  )

df_addtl <- df_addtl |>
  group_by(period) |>
  summarise(
    n_sessions = n_distinct(cbv_flow_id, na.rm = TRUE),
    n_applicants = n_distinct(distinct_id, na.rm = TRUE),
    n_completed = n_distinct(completed_with_id, na.rm = TRUE),
    duration_complete = median(session_time, na.rm = TRUE),
    duration_sync = median(sync_duration_seconds, na.rm = TRUE),
    share_smartphone = n_distinct(smartphone_with_id, na.rm = TRUE) /
      n_sessions,
    share_gig = n_distinct(gig_with_id, na.rm = TRUE) / n_completed,
    .groups = "drop"
  )

#reshape
df_addtl <- df_addtl |>
  pivot_longer(
    -period,
    names_to = "metric"
  ) |>
  pivot_wider(
    names_from = period
  )

if (!"prior" %in% names(df_addtl)) {
  df_addtl <- df_addtl |>
    mutate(prior = NA_real_)
}

#rename for table
df_addtl <- df_addtl |>
  mutate(
    metric_clean = recode_values(
      metric,
      "n_sessions" ~ "Number of Sessions",
      "n_applicants" ~ "Distinct Applicants",
      "n_completed" ~ "Number of Completed Sessions",
      "duration_complete" ~ "Time to Complete (Median, Min.)",
      "duration_sync" ~ "Payroll Provider Sync Time (Median, Sec.)",
      "share_smartphone" ~ "Share Smartphones (Sessions)",
      "share_gig" ~ "Share Gig (Sessions of Completed)"
    ),
    across(
      where(is.numeric),
      ~ case_when(
        str_detect(metric, "share") ~ label_percent(1)(.x),
        metric == "duration_complete" ~ label_timespan(
          1,
          unit = "mins",
          space = FALSE
        )(.x),
        metric == "duration_sync" ~ label_timespan(
          1,
          unit = "secs",
          space = FALSE
        )(.x),
        TRUE ~ label_comma(1)(.x)
      )
    ),
    .after = metric
  )

write_parquet(df_addtl, "Dataout/report_addtl.parquet")

# BAN --------------------------------------------------------------------

#event recoding map
df_lookup <- tribble(
  ~from                                 , ~to                  ,
  "ApplicantViewedAgreement"            , "n_started"          ,
  "ApplicantSharedIncomeSummary"        , "n_completed"        ,
  "ApplicantSearchedForEmployer"        , "n_searches"         ,
  "ApplicantAccessedMissingResultsPage" , "n_searches_missing"
)

#subset to start and finish
df_ban <- df_mp |>
  filter(event %in% df_lookup$from) |>
  mutate(
    event_ban = recode_values(event, from = df_lookup$from, to = df_lookup$to),
    started = event == "ApplicantViewedAgreement",
    started_with_id = case_when(
      event == "ApplicantViewedAgreement" ~ distinct_id
    ),
    completed_with_id = case_when(
      event == "ApplicantSharedIncomeSummary" ~ distinct_id
    ),
    searches = event == "ApplicantSearchedForEmployer",
    searches_missing = event == "ApplicantAccessedMissingResultsPage",
    session_time = case_when(
      event == "ApplicantSharedIncomeSummary" ~ flow_started_minutes_ago
    )
  )

df_ban <- df_ban |>
  group_by(period) |>
  summarise(
    n_started = n_distinct(started_with_id, na.rm = TRUE),
    share_completed = n_distinct(completed_with_id, na.rm = TRUE) /
      n_distinct(started_with_id, na.rm = TRUE),
    duration_complete = median(session_time, na.rm = TRUE),
    share_missing_searches = sum(searches_missing, na.rm = TRUE) /
      sum(searches, na.rm = TRUE),
    .groups = "drop"
  )

df_ban <- df_ban |>
  pivot_longer(
    matches("^(n|share|duration)_"),
    names_to = "metric"
  ) |>
  pivot_wider(
    names_from = period
  )

if (!"prior" %in% names(df_ban)) {
  df_ban <- df_ban |>
    mutate(prior = NA_real_)
}

df_ban <- df_ban |>
  mutate(
    delta = (curr / prior) - 1
  )


df_ban <- df_ban |>
  mutate(
    label = recode_values(
      metric,
      "n_started" ~ "Verifications\nStarted",
      "share_completed" ~ "Completion\nRate",
      "duration_complete" ~ "Time to\nComplete",
      "share_missing_searches" ~ "Missing\nSearch Results"
    ),
    curr_fmt = case_when(
      str_detect(metric, "share") ~ label_percent(1)(curr),
      metric == "duration_complete" ~ label_number(
        .1,
        suffix = "m"
      )(curr),
      # metric == "duration_complete" ~ label_timespan(
      #   .1,
      #   unit = "mins",
      #   space = FALSE
      # )(curr),
      TRUE ~ label_number(.1, scale_cut = cut_short_scale())(curr)
    ),
    icon = case_when(
      delta > 0 ~ "bi-caret-up-fill",
      delta < 0 ~ "bi-caret-down-fill"
    ),
    delta_fmt = case_when(
      is.na(delta) ~ "No prior period",
      between(delta, -.005, .005) ~ "No change",
      TRUE ~ str_glue(
        '<i class="bi {icon}"></i> {label_percent(1)(abs(delta))} from last {report_type}' # {text} than prior run
      )
    )
  )


write_parquet(df_ban, "Dataout/report_ban.parquet")

# TREND ------------------------------------------------------------------

df_lookup_trend <- tribble(
  ~from                              , ~to                              ,
  "CaseworkerInvitedApplicantToFlow" , "Invitations Sent to Applicants" ,
  "ApplicantViewedAgreement"         , "Applicants Viewing Agreement"   ,
  "ApplicantSharedIncomeSummary"     , "Applicants Sharing Summary"
)

df_trend <- df_mp |>
  filter(
    event %in% df_lookup_trend$from,
    timestamp >= today() - months(1),
  ) |>
  mutate(
    event = event |>
      recode_values(from = df_lookup_trend$from, to = df_lookup_trend$to) |>
      factor(df_lookup_trend$to)
  ) |>
  count(event, date = as_date(timestamp))


write_parquet(df_trend, "Dataout/report_trend.parquet")


# SEARCHES ---------------------------------------------------------------

df_search <- df_mp |>
  filter(
    !event %in%
      c(
        "ApplicantViewedHelpText",
        "ApplicantOpenedHelpModal",
        "ApplicantViewedHelpTopic"
      )
  ) |>
  clean_events() |>
  mutate(
    event = event |>
      as.factor() |>
      fct_relevel(
        "ApplicantSelectedEmployerOrPlatformItem",
        "ApplicantBeganLinkingEmployer"
      )
  ) |>
  arrange(distinct_id, timestamp, event) |>
  group_by(cbv_flow_id) |>
  mutate(lead_event = lead(event_clean)) |>
  ungroup() |>
  filter(event == "ApplicantSearchedForEmployer")


df_search <- df_search |>
  mutate(
    query_clean = query |>
      str_to_lower() |> # already done
      str_trim() |> # remove leading/trailing whitespace
      str_squish() |> # collapse internal whitespace
      str_remove_all("[^a-z0-9 ]") |> # remove punctuation/special chars
      str_replace_all("\\b(inc|llc|ltd|co|corp|the)\\b", "") |> # strip common suffixes
      str_squish() # squish again after removals
  )

# df_search |>
#   count(lead_event, sort = TRUE)

df_searches_top <- df_search |>
  mutate(is_successful = lead_event == "Selected Employer") |>
  filter_out(is.na(is_successful)) |>
  group_by(is_successful) |>
  count(query_clean, sort = TRUE) |>
  slice_head(n = 10) |>
  ungroup() |>
  mutate(event_count = str_glue("{toupper(query_clean)} ({n})"))


write_parquet(df_searches_top, "Dataout/report_searches.parquet")
