gb <- 5000*1024^2
options(future.globals.maxSize=gb)
library(fastverse)
# library(future)
library(furrr)
library(progressr)
# library(foreach)
# library(listenv)
source("R/functions.R")
handlers(global = TRUE)

## Set for parallel processing
## Keep half cores for processes
## And the other half for sending parallel requests
# n_cores <- floor((availableCores() - 1) / 2)

# Parameters -----------
version  <- "20230919_2017_01_02_PROD"
version  <- "20230919_2011_02_02_PROD"
# folder   <- "PIP_estimates_for_Country_profile"
folder   <- "1kbins_survey_years"

new_dir <-
  fs::path("p:/03.pip/estimates", folder, version) |>
  fs::dir_create(recurse = TRUE)


data_dir <- Sys.getenv("PIPAPI_DATA_ROOT_FOLDER_LOCAL") |>
  fs::path(version)

est_dir <- fs::path(data_dir, "estimations")
svy_dir <- fs::path(data_dir, "survey_data")
aux_dir <- fs::path(data_dir, "_aux")


# load data ---------


svy_mns <-
  est_dir |>
  fs::path("survey_means", ext = "fst") |>
  fst::read_fst(as.data.table = TRUE)


svy_files_path <-
  fs::dir_ls(svy_dir)

filter <- 1:250
filter <- 1:length(svy_files_path)
svy_files_path <- svy_files_path[filter]

svy_cache_id <-
  svy_files_path |>
  fs::path_file() |>
  fs::path_ext_remove()


lt <-
  svy_files_path |>
  lapply(\(.) {
    fst::read_fst(., as.data.table = TRUE)
  })

names(lt) <- svy_cache_id


pls <- c(seq(from = 0.05, to = 10, by = 0.05),
         seq(from = 11, to = 21, by = 1),
         21.7)


# pls <- 1:5

# furrr -----------
# plan(list(sequential, multisession))

## estimations ---------
# plan(multisession)
n_cores <- floor((availableCores() + 1) / 2)
plan(multisession, workers = n_cores)

pip_estimates <- function(id, svy_mns, lt, pls, p = NULL) {
  if (!is.null(p)) p()
  sm <- svy_mns[cache_id == id]
  dt <- lt[[id]]

  lapply(pls, \(.) {
    rg_pip_est(., dt, sm, id)
  }) |>
    rbindlist()
}

# pip_estimates(svy_cache_id[1], svy_mns, lt, pls[1])

tictoc::tic()
with_progress({
  p    <- progressor(steps = length(svy_cache_id))
  ctrs <- future_map(svy_cache_id, pip_estimates, svy_mns, lt, pls, p)
})
tictoc::toc()
plan(sequential)


## send message -----
if (require(pushoverr)) {
  pushoverr::pushover("Done pip estimates for Pov GP")
}

## formating -----------
pip_est <- rbindlist(ctrs, fill = TRUE)
vars <-
  c("country_code",
    "year",
    "survey_acronym",
    "welfare_type")
pip_est[,
        (vars) := tstrsplit(cache_id,
                            split = "_",
                            keep = c(1:3, 5))
]

## Save --------

est_file_name <- fs::path(new_dir, "pip_estimates")
qs::qsave(ctrs, fs::path(est_file_name, ext = "qs"))
fst::write_fst(pip_est, fs::path(est_file_name, ext = "fst"))
haven::write_dta(pip_est, fs::path(est_file_name, ext = "dta"))



# foreach --------

parallel::detectCores()
n.cores <- parallel::detectCores() - 1
my.cluster <- parallel::makeCluster(
  n.cores,
  type = "PSOCK"
)
doParallel::registerDoParallel(cl = my.cluster)
opts <- list(chunkSize = 10)

tictoc::tic()
my_est <- function(ids, svy_mns, lt, pls) {
  p <- progressor(along = ids)

  ctrs <- foreach(id = ids,
                  .options.nws=opts
  ) %dopar% {
    p()
    sm <- svy_mns[cache_id == id]
    dt <- lt[[id]]

    lapply(pls, \(.) {
      rg_pip_est(., dt, sm, id)
    }) |>
      rbindlist()
  }
}
my_est(svy_cache_id,  svy_mns, lt, pls)

tictoc::toc()

