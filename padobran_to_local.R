library(data.table)
library(lubridate)


# SETUP -------------------------------------------------------------------
# Globals
if (interactive()) {
  PATH = "D:/strategies/exuber"
}

# Create indicators directory if it doesn't exist
if (interactive()) {
  if (!dir.exists(file.path(PATH, "indicators"))) {
    dir.create(file.path(PATH, "indicators"))
  }
}


# DATA --------------------------------------------------------------------
# Check columns
# colnames(fread(list.files(file.path(PATH, "predictors"), full.names = TRUE)[1]))

# Current column
# if (interactive()) {
#   i = 1L
# } else {
#   i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
# }
# i = i + 2L
# cols = c(1, 2, i)

# Get files
if (interactive()) {
  files = list.files(file.path(PATH, "predictors"), full.names = TRUE)
} else {
  files = list.files("predictors", full.names = TRUE)
}

# Import predictors in chunks Predictors
system.time({
  dt = lapply(files, fread) # , select = cols
})
format(object.size(dt), units = "auto")
dt = rbindlist(dt, fill= TRUE)
dt[, date := with_tz(date, tzone = "America/New_York")]
setorder(dt, symbol, date)

# Median aggreagation
radf_vars = colnames(dt)[grepl("exuber", colnames(dt))]
indicators_median = dt[, lapply(.SD, median, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_median, radf_vars, paste0("median_", radf_vars))

# Standard deviation aggregation
indicators_sd = dt[, lapply(.SD, sd, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_sd, radf_vars, paste0("sd_", radf_vars))

# Mean aggregation
indicators_mean = dt[, lapply(.SD, mean, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_mean, radf_vars, paste0("mean_", radf_vars))

# Sum aggreagation
indicators_sum = dt[, lapply(.SD, sum, na.rm = TRUE), by = c('date'), .SDcols = radf_vars]
setnames(indicators_sum, radf_vars, paste0("sum_", radf_vars))

# Expected Shartfall
indicators_es = dt[, lapply(.SD, function(x) {
  if (length(x) < 2) return(NA_real_)
  PerformanceAnalytics::ES(diff(x), p = 0.05, method = "modified")
}), by = date, .SDcols = radf_vars]

# Merge indicators
indicators = Reduce(
  function(x, y)
    merge(
      x,
      y,
      by = "date",
      all.x = TRUE,
      all.y = TRUE
    ),
  list(
    indicators_sd,
    indicators_median,
    indicators_sum,
    indicators_mean
  )
)
setorder(indicators, date)

# Remove missing values
indicators = na.omit(indicators)

# Save indicators
fwrite(indicators, "indicators.csv")
