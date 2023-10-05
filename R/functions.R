#' regular estimation of PIP stats
#'
#' @param pl poverty lines
#' @param dt data frame with welfare and weight variables
#' @param sm survey means data frame from /estimations folder
#' @param id cache id
#'
#' @return data.frame
rg_pip_est <- function(pl, dt, sm, id) {

  #repoting type
  rt <- id |>
    strsplit(split = "_") |>
    unlist() |>
    {\(.) .[4]}()

  y <- vector("list", length = nrow(sm))

  for (i in seq_along(nrow(sm))) {
    dist_type        <- sm$distribution_type[i]
    mean_ppp         <- sm$survey_mean_ppp[i]
    mean_lcu         <- sm$survey_mean_lcu[i]
    ppp              <- sm$ppp[i]
    reporting_level  <- sm$reporting_level[i]

    if (rt == "D1") {
      dt2 <- copy(dt)
    } else {
      if (reporting_level  != "national") {
        dt2 <- dt[area == reporting_level ]
      } else {
        next
      }
    }

    y[[i]] <-
      wbpip:::prod_compute_pip_stats(
        welfare           = dt2$welfare,
        population        = dt2$weight,
        povline           = pl,
        popshare          = NULL,
        requested_mean    = mean_ppp,
        svy_mean_lcu      = mean_lcu,
        svy_median_lcu    = 1,
        svy_median_ppp    = 1,
        default_ppp       = ppp,
        ppp               = NULL,
        distribution_type = dist_type
      ) |>
      as.data.table()
  }
  y <- rbindlist(y)
  y[, `:=`(
    cache_id        = id,
    reporting_level = reporting_level
  )]

  if (any(names(y) == "median")) {
    y[, median := NULL]
  }
  y
}


