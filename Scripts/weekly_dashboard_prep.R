library(tidyverse)
library(emmytics)
library(glue)
library(scales)
library(here)

#get new data
curr_pilot_info <- pilot_pds |>
  filter_out(start_date > today()) |>
  rowwise() |>
  mutate(,
    api_end = min(today(), end_date),
    path_dates = str_glue("{start_date}_to_{api_end}")
  ) |>
  ungroup() |>
  mutate(prior_path_dates = lag(path_dates)) |>
  slice_tail() |>
  select(start_date, api_end, client_agency, path_dates, prior_path_dates)

#access data
# curr_pilot_info |>
#   pmap(~ get_mixpanel_data(..1, ..2, ..3, cache_dir = "Data"))

#data path
paths <- list.files(
  "Data",
  str_glue("({curr_pilot_info$path_dates}|{curr_pilot_info$prior_path_dates})"),
  full.names = TRUE
)

#read in data
df <- paths |>
  map(
    ~ read_mixpanel(
      .x,
      device_type = properties$device_type,
      origin = properties$origin,
      sync_duration_seconds = properties$sync_duration_seconds,
      flow_started_seconds_ago = properties$flow_started_seconds_ago,
      # flow_started_minutes_ago = properties$flow_started_seconds_ago / 60,
      applicant_only = FALSE,
      drop_prop = TRUE
    )
  ) |>
  list_rbind()

#clean up col types
df <- df |>
  mutate(
    cbv_flow_id = as.integer(cbv_flow_id),
    across(c(sync_duration_seconds, flow_started_seconds_ago), as.numeric)
  )

#convert seconds to minutes
df <- df |>
  mutate(flow_started_seconds_ago = flow_started_seconds_ago / 60) |>
  rename(flow_started_minutes_ago = flow_started_seconds_ago)

# add pilot week
df <- add_pilot_week(df)

#identify the latest pilot
pilot_latest <- return_latest_pilot(df)

#identify the prior pilot
pilot_prior <- pilot_pds |>
  filter(state == unique(df$pilot_state)) |>
  mutate(prior = lag(pilot)) |>
  filter(pilot == pilot_latest) |>
  pull()

########
# MANUAL CHANGE >>>>>>>>>>>>>>>>
#######
df <- df |> filter_out(pilot == "Feb 2026", pilot_wk > 2)

#identify pilot week
pilot_week <- df |>
  filter(pilot == pilot_latest) |>
  distinct(pilot_wk) |>
  pull() |>
  max()


#header context
# - # of emailed

#BAN
# - started #
# - completion rate %
# - share tokenized links sessions? %
# - errors? #
# - missed searches? %
# - help actions? #

#plots
# - access point (phone/desktop)
# - sync times
# - total time

#event recoding map
df_lookup <- tribble(
  ~from                                        , ~to                          ,
  "ApplicantClickedCBVInvitationLink"          , "initiation_token"           ,
  "ApplicantClickedGenericLink"                , "initiation_generic"         ,
  "ApplicantRestartedWithGenericLinkOnTimeout" , "initiation_generic_restart" ,
  "ApplicantViewedAgreement"                   , "started"                    ,
  "ApplicantSharedIncomeSummary"               , "completed"                  ,
  "ApplicantOpenedHelpModal"                   , "help"                       , #help_menu
  "ApplicantViewedHelpTopic"                   , "help"                       , #help_common
  "ApplicantSearchedForEmployer"               , "search"                     ,
  "ApplicantAccessedMissingResultsPage"        , "search_missing"             ,
  "ApplicantEncounteredError"                  , "error"                      ,
  "CaseworkerInvitedApplicantToFlow"           , "invitation"
)

#subset to start and finish
df_ban <- df |>
  filter(event %in% df_lookup$from) |>
  mutate(
    event_ban = recode_values(event, from = df_lookup$from, to = df_lookup$to)
  )

#agg to event + applicant level and reshape events long
df_ban <- df_ban |>
  group_by(pilot_state, pilot, pilot_wk, event_ban) |>
  summarise(
    applicants = n_distinct(distinct_id),
    events = n(),
    .groups = "drop"
  ) |>
  pivot_longer(c(applicants, events), names_to = "denom") |>
  pivot_wider(
    names_from = event_ban,
    values_fill = 0
  )

#calculations
df_ban <- df_ban |>
  mutate(
    share_complete = completed / started,
    share_tokenized = initiation_token /
      (initiation_generic + initiation_generic_restart + initiation_token),
    share_search_missing = search_missing / search
  ) |>
  rename_with(
    ~ paste0("n_", .x),
    .cols = c(started, error, starts_with("help"), invitation)
  ) |>
  select(pilot_state, pilot, pilot_wk, denom, matches("^(n|share)_"))

#format
df_ban <- df_ban |>
  mutate(pilot2 = ifelse(pilot == pilot_latest, "curr", "prior")) |>
  select(-pilot) |>
  pivot_longer(
    matches("^(n|share)_"),
    names_to = "metric"
  ) |>
  pivot_wider(names_from = pilot2) |>
  mutate(
    delta = curr - prior,
    delta_pct = delta / prior,
    val_fmt = ifelse(
      str_detect(metric, "^n_"),
      label_comma(1)(curr),
      label_percent(1)(curr)
    ),
    icon = case_when(
      str_detect(metric, "^n_") & delta > 0 ~ "bi-caret-up-fill",
      str_detect(metric, "^n_") & delta < 0 ~ "bi-caret-down-fill",
      # TRUE ~ "bi-dash"
    ),
    text = case_when(
      delta > 0 ~ "more",
      delta < 0 ~ "less"
    ),
    text_fmt = case_when(
      str_detect(metric, "^n_") & between(delta_pct, -.5, .5) ~ "No change",
      str_detect(metric, "^share") &
        between(delta_pct, -.005, .005) ~ "No change",
      str_detect(metric, "^n_") ~ str_glue(
        '<i class="bi {icon}"></i> {label_comma(1)(abs(delta))}' # {text} than prior run
      ),
      str_detect(metric, "^share_") ~ str_glue(
        '<i class="bi {icon}"></i> {label_percent(1)(abs(delta))} (points)' #points {text} than prior run
      ),
      TRUE ~ "No change"
    )
  )

#export
write_rds(df_ban, "Dataout/wkly_ban.rds")

###################
###################

#device type
#flag if the applicant started on a tokenized or generic link
df_device_origin_status <- df |>
  mutate(
    initiation = case_when(
      event %in%
        c(
          "ApplicantClickedCBVInvitationLink",
          "ApplicantClickedGenericLink"
        ) ~ str_extract(event, "CBVInvitation|ClickedGenericLink")
    ),
    initiation = initiation |>
      str_replace_all("(?<!^)([A-Z])", " \\1") |>
      str_remove('C B V ')
  )

#fill missing across flow - initiation, device, and origin
df_device_origin_status <- df_device_origin_status |>
  group_by(distinct_id, cbv_flow_id) |>
  fill(device_type, origin, initiation, .direction = "updown") |>
  ungroup()

#clean up columns for viz
df_device_origin_status <- df_device_origin_status |>
  mutate(
    across(
      c(device_type, origin, initiation),
      ~ ifelse(is.na(.), "unknown", .)
    ),
    device_type = ifelse(
      device_type %in% c("desktop", 'smartphone', 'unknown'),
      device_type,
      "other"
    )
  )


df_device_origin_status <- df_device_origin_status |>
  filter(
    event %in% c("ApplicantViewedAgreement", "ApplicantSharedIncomeSummary"),
    # pilot %in% c(pilot_latest, pilot_prior),
    # pilot_wk == pilot_week
  ) |>
  mutate(
    started = event == "ApplicantViewedAgreement",
    completed = event == "ApplicantSharedIncomeSummary",
  )

#aggregate to flow level
df_device_origin_status <- df_device_origin_status |>
  group_by(
    pilot,
    pilot_wk,
    distinct_id,
    cbv_flow_id,
    device_type,
    origin,
    initiation
  ) |>
  summarise(
    across(c(started, completed), ~ sum(.x, na.rm = TRUE)),
    .groups = "drop"
  )

#aggregate to weekly level
df_device_origin_status <- df_device_origin_status |>
  group_by(pilot, pilot_wk, device_type) |>
  summarise(
    across(c(started, completed), ~ sum(., na.rm = TRUE)),
    .groups = "drop"
  ) |>
  mutate(completion_rate = completed / started)

write_rds(df_device_origin_status, "Dataout/wkly_device_origin.rds")

###################
###################

#sync + duration
df_duration <- df |>
  filter(
    event %in% c("ApplicantFinishedSync", "ApplicantSharedIncomeSummary"),
    # pilot %in% c(pilot_latest, pilot_prior),
    # pilot_wk == pilot_week
  ) |>
  select(-c(device_type, origin))

#reshape
df_duration <- df_duration |>
  pivot_longer(
    c(sync_duration_seconds, flow_started_minutes_ago),
    names_to = "metric",
    values_drop_na = TRUE
  )

df_duration <- df_duration |>
  group_by(pilot, pilot_wk, metric, provider) |>
  summarise(
    n = n(),
    min = min(value, na.rm = TRUE),
    q25 = quantile(value, 0.25, na.rm = TRUE),
    median = median(value, na.rm = TRUE),
    q75 = quantile(value, 0.75, na.rm = TRUE),
    max = max(value, na.rm = TRUE),
    .groups = "drop"
  )

write_rds(df_duration, "Dataout/wkly_duration.rds")

# df_sync_bucketed <- df_sync |>
#   mutate(
#     sync_bucket =
#       case_when(
#         sync_duration_seconds <= 30 ~ "<=30s",
#         sync_duration_seconds <= 60 ~ ">30s - 1m",
#         sync_duration_seconds <= 180 ~ ">1m - 3m",
#         sync_duration_seconds <= 300 ~ ">3m - 5m",
#         sync_duration_seconds <= 600 ~ ">5m - 10m",
#         TRUE ~ ">10m"
#         ),
#       sync_bucket = factor(sync_bucket, c("<=30s", ">30s - 1m", ">1m - 3m", ">3m - 5m", ">5m - 10m", ">10m"))
#   ) |>
#   count(pilot, pilot_wk, sync_bucket) |>
#   arrange(pilot, sync_bucket) |>
#   group_by(pilot) |>
#   mutate(
#     share = n / sum(n),
#     cum_share = cumsum(n) / sum(n)
#     ) |>
#   ungroup()

#   df_sync_bucketed |>
#     ggplot(aes(n, fct_rev(sync_bucket), fill = sync_bucket)) +
#     geom_col() +
#     geom_text(
#       aes(label = scales::label_percent(1)(share)),
#       family = "Source Sans 3", hjust = -.1, color = "#505050",
#       ) +
#     facet_wrap(~pilot_wk) +
#     coord_cartesian(clip = "off") +
#     # scale_fill_manual(values = unname(c(dsac_color['teal'], dsac_color['dark_navy'], dsac_color['navy'], dsac_color['light_navy'], dsac_color['light_cranberry'], dsac_color['cranberry']))) +
#     labs(
#       x = NULL, y = NULL,
#       title = "95% OF SYNC TIMES ARE UNDER THREE MINUTES",
#       subtitle = "number of sessions' income sync time",
#       # caption = default_caption
#       ) +
#     # si_style_xgrid() +
#     theme(legend.position = "none")
# #origin
