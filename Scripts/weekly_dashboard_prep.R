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
  # str_glue("({curr_pilot_info$path_dates}|{curr_pilot_info$prior_path_dates})"),
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
      employer_name = properties$employment_employer_name,
      employment_type = properties$employment_type,
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
df <- df |> filter_out(pilot == "Feb 2026", pilot_wk > 3)

#identify pilot week
pilot_week <- df |>
  filter(pilot == pilot_latest) |>
  distinct(pilot_wk) |>
  pull() |>
  max()

#include a cumulative week coded -1
df <- df |>
  bind_rows(
    df |>
      filter(between(pilot_wk, 1, pilot_week)) |>
      mutate(pilot_wk = -1)
  )


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
  "ApplicantAttemptedLogin"                    , "login_attempt"              ,
  "ApplicantSucceededWithLogin"                , "login_success"              ,
  "ApplicantOpenedHelpModal"                   , "help"                       , #help_menu
  "ApplicantViewedHelpTopic"                   , "help"                       , #help_common
  "ApplicantSearchedForEmployer"               , "search"                     ,
  "ApplicantAccessedMissingResultsPage"        , "search_missing"             ,
  "ApplicantEncounteredError"                  , "error"                      ,
  "CaseworkerInvitedApplicantToFlow"           , "invitation"                 ,
  "ApplicantFinishedSync"                      , "synced"                     ,
  "ApplicantCopiedInvitationLink"              , "copied_invite"
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

#identify number of multi employer submissions
df_multi <- df |>
  filter(event == "ApplicantFinishedSync") |>
  mutate(
    employer_name = ifelse(
      is.na(employer_name),
      "[not captured]",
      employer_name
    )
  ) |>
  distinct(pilot_state, pilot, pilot_wk, distinct_id, employer_name) |>
  count(pilot_state, pilot, pilot_wk, distinct_id, name = "submissions") |>
  count(pilot_state, pilot, pilot_wk, submissions > 1) |>
  filter(`submissions > 1` == TRUE) |>
  mutate(denom = "applicants") |>
  select(pilot_state, pilot, pilot_wk, denom, multi_employers = n)

#merge on multi employer submissions
df_ban <- df_ban |>
  left_join(df_multi, by = join_by(pilot_state, pilot, pilot_wk, denom))

#calculations
df_ban <- df_ban |>
  mutate(
    share_complete = completed / started,
    share_tokenized = initiation_token /
      (initiation_generic + initiation_generic_restart + initiation_token),
    share_search_missing = search_missing / search,
    share_login_success = login_success / login_attempt
  ) |>
  rename_with(
    ~ paste0("n_", .x),
    .cols = c(
      started,
      error,
      starts_with("help"),
      invitation,
      copied_invite,
      multi_employers
    )
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
      delta > 0 ~ "bi-caret-up-fill",
      delta < 0 ~ "bi-caret-down-fill",
      # TRUE ~ "bi-dash"
    ),
    text = case_when(
      delta > 0 ~ "more",
      delta < 0 ~ "less"
    ),
    text_fmt = case_when(
      between(delta_pct, -.005, .005) ~ "No change",
      str_detect(metric, "^n_") ~ str_glue(
        '<i class="bi {icon}"></i> {label_comma(1)(abs(delta))} from last run' # {text} than prior run
      ),
      str_detect(metric, "^share_") ~ str_glue(
        '<i class="bi {icon}"></i> {label_percent(1)(abs(delta))} points from last run' #points {text} than prior run
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

#setup for viz
df_device_origin_status <- df_device_origin_status |>
  group_by(pilot, pilot_wk) |>
  mutate(share = started / sum(started)) |>
  ungroup() |>
  mutate(
    device_type = factor(
      device_type,
      c("desktop", "other", "unknown", "smartphone")
    ),
    completion_rate = label_percent(1)(completion_rate),
    lab_share = label_percent(1)(share),
    fill_color = case_when(
      device_type == "smartphone" ~ dsac_light_teal,
      device_type == "desktop" ~ dsac_light_navy,
      TRUE ~ "#909090"
    ),
    lab_share = case_when(
      device_type %in% c("smartphone", "desktop") ~ str_glue(
        "{str_to_sentence(device_type)}\ndevice share: {lab_share}\ncompletion rate: {completion_rate}"
      )
    ),
    lab_pos = case_when(
      device_type == "smartphone" ~ -.02,
      device_type == "desktop" ~ 1.02
    ),
    lab_hjust = case_when(
      device_type == "smartphone" ~ 1,
      device_type == "desktop" ~ 0
    ),
  ) |>
  arrange(device_type) |>
  mutate(fill_color = fct_inorder(fill_color))


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
  select(-c(device_type, origin, employer_name, provider))

#reshape
df_duration <- df_duration |>
  pivot_longer(
    c(sync_duration_seconds, flow_started_minutes_ago),
    names_to = "metric",
    values_drop_na = TRUE
  )

df_duration <- df_duration |>
  group_by(pilot, pilot_wk, metric, employment_type) |> #provider
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


#falloff

df_dropoff <- df |>
  filter(event %in% key_events)

df_dropoff <- df_dropoff |>
  distinct(pilot, pilot_wk, distinct_id, event)

df_dropoff <- df_dropoff |>
  clean_events() |>
  mutate(
    pilot_rel = ifelse(pilot == pilot_latest, "curr", "prior"),
    event_clean = factor(event_clean, key_events_clean)
  ) |>
  count(pilot_rel, pilot_wk, event_clean) |>
  pivot_wider(names_from = pilot_rel, values_from = n)

df_dropoff <- df_dropoff |>
  group_by(pilot_wk) |>
  mutate(
    curr_lag = lag(curr),
    loss_share = label_percent(1)(curr / curr_lag - 1)
  ) |>
  ungroup()

df_dropoff <- df_dropoff |>
  mutate(event_clean = fct_recode(event_clean, "Consented" = "Agreed"))

write_rds(df_dropoff, "Dataout/wkly_dropoff.rds")


#Viz

df_daily <- df |>
  filter(
    # pilot == pilot_latest,
    between(pilot_wk, 1, pilot_week),
    event == "ApplicantViewedAgreement"
  ) |>
  group_by(pilot, pilot_wk) |>
  mutate(week_total = n()) |>
  ungroup() |>
  mutate(
    day = as_date(timestamp),
    fill_color = ifelse(pilot_wk == pilot_week, dsac_light_teal, "#909090"),
    pilot_wk = str_glue(
      "Week {pilot_wk} [n = {label_number(scale_cut = cut_short_scale())(week_total)}]"
    )
  ) |>
  count(pilot, pilot_wk, day, fill_color)

v_sub <- df_daily |>
  count(pilot, wt = n) |>
  mutate(
    pilot_cum = str_glue(
      "{label_number(accuracy = 1, scale_cut = cut_short_scale())(n)} cum. in {pilot}"
    )
  ) |>
  pull() |>
  paste(collapse = " vs. ")

df_daily |>
  ggplot(aes(day, n, fill = fill_color)) +
  geom_col() +
  facet_wrap(
    pilot ~ pilot_wk,
    nrow = 2,
    scales = "free_x",
    labeller = labeller(.multi_line = FALSE)
  ) +
  # facet_wrap(~pilot_wk, space = "free_x", scales = "free_x") +
  scale_x_date(date_breaks = "1 day", date_labels = "%m/%d") +
  scale_y_continuous(label = label_number(scale_cut = cut_short_scale())) +
  scale_fill_identity() +
  labs(
    x = NULL,
    y = NULL,
    title = toupper("Number of Income Verifications Initiated"),
    subtitle = v_sub,
  ) +
  glitr::si_style_ygrid() +
  theme(panel.spacing = unit(1, "lines"))

glitr::si_save("Images/initations.png")


#submissions
df |>
  filter(
    pilot == pilot_latest,
    pilot_week > 0,
    event %in% c("ApplicantViewedAgreement", "ApplicantSharedIncomeSummary")
  ) |>
  group_by(pilot, event) |>
  summarise(
    applicants = n_distinct(distinct_id),
    events = n(),
    .groups = "drop"
  ) |>
  pivot_longer(c(applicants, events), names_to = "denom") |>
  pivot_wider(
    names_from = event,
    values_fill = 0
  ) |>
  mutate(share = ApplicantSharedIncomeSummary / ApplicantViewedAgreement)
