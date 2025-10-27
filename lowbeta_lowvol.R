library(fastverse)
library(finutils)
library(fracdiff)
library(roll)
library(ggplot2)
library(PerformanceAnalytics)
library(AzureStor)
library(qlcal)
library(RollingWindow)
library(ggplot2)
library(MASS)


# Setup
setCalendar("UnitedStates/NYSE")
PATH_LEAN = "C:/Users/Mislav/qc_snp/data"

file.path(PATH_LEAN, "equity", "usa", "universes", c("spy", "iwm"))
# Prices data
prices = qc_daily_parquet(
  file_path = file.path(PATH_LEAN, "all_stocks_daily"),
  etfs = FALSE,
  # etf_cons = file.path(PATH_LEAN, "equity", "usa", "universes", c("spy", "iwm")),
  min_obs = 252,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  market_symbol = "spy"
  # profiles_fmp = TRUE,
  # fmp_api_key = Sys.getenv("APIKEY")
)
prices[, month := data.table::yearmon(date)]

# Remove ETFS
# nrow(prices[isEtf == TRUE | isFund == TRUE]) / nrow(prices)
# prices = prices[isEtf == FALSE & isFund == FALSE]

# Calculate rolling beta
setorder(prices, symbol, date)
prices[, beta_roll_year     := RollingWindow::RollingBeta(spy_returns, returns, window = 252), by = symbol]
prices[, beta_roll_halfyear := RollingWindow::RollingBeta(spy_returns, returns, window = 125), by = symbol]
prices[, beta_roll_month    := RollingWindow::RollingBeta(spy_returns, returns, window = 22), by = symbol]
weights_ = rep(1 / 3, 3)
prices[, beta_average_year :=
         beta_roll_year * weights_[3] +
         beta_roll_halfyear * weights_[2] +
         beta_roll_month * weights_[1]
]
hist(prices[, beta_average_year])
prices[, sum(is.na(beta_average_year)) / nrow(prices) * 100]

# Rolling sd
setorder(prices, symbol, date)
prices[, sd_roll_year := roll::roll_sd(returns, width = 252), by = symbol]
prices[, sd_roll_halfyear := roll::roll_sd(returns, width = 125), by = symbol]
prices[, sd_roll_month := roll::roll_sd(returns, width = 22), by = symbol]
weights_ = c(252, 150, 22) / sum(c(252, 150, 22))
prices[, sd_average_year :=
         sd_roll_year * weights_[3] +
         sd_roll_halfyear * weights_[2] +
         sd_roll_month * weights_[1]
]

# Momentum
setorder(prices, symbol, date)
months_size = 2:12
mom_vars = paste0("momentum_", months_size)
f_ = function(x, n) {
  shift(x, 21) / shift(x, n * 21) - 1
}
prices[, (mom_vars) := lapply(months_size, function(x) f_(close, x)), by = symbol]
weights_ = c(12:2) / sum(12:2)
prices[, momentum_average_year :=
         momentum_2 * weights_[1] +
         momentum_3 * weights_[2] +
         momentum_4 * weights_[3] +
         momentum_5 * weights_[4] +
         momentum_6 * weights_[5] +
         momentum_7 * weights_[6] +
         momentum_8 * weights_[7] +
         momentum_9 * weights_[8] +
         momentum_10 * weights_[9] +
         momentum_11 * weights_[10] +
         momentum_12 * weights_[11]
]
prices[, mom := momentum_average_year / sd_average_year]

# (Optional but recommended) standardize momentum each date
prices[, mom_z := scale(mom), by = date]

# Rolling time series regressions (by symbol)
iv = function(dt, window = 22) {
  # Column names with window suffix
  iv_col    = paste0("IV_", window)
  ivol_col  = paste0("IVOL_", window)
  iskew_col = paste0("ISKEW_", window)
  gmb_col   = paste0("GMB_", window)
  resid_col = paste0("resid_", window)

  # 1) rolling regression
  dt[, c("b0","b_mkt","b_mom") := {
    fit = roll_lm(
      x = cbind(spy_returns, mom),
      y = returns,
      width = window,
      intercept = TRUE,
      online = FALSE
    )
    as.data.table(fit$coefficients)
  }, by = symbol]

  # 2) residuals
  dt[, (resid_col) := returns - (b0 + b_mkt * spy_returns + b_mom * mom)]

  # 3) IV / IVOL / ISKEW
  dt[, (iv_col) := frollmean(get(resid_col)^2, n = window), by = symbol]
  dt[, (ivol_col) := sqrt(get(iv_col))]
  dt[, m3 := frollmean(get(resid_col)^3, n = window), by = symbol]
  dt[, (iskew_col) := fifelse(get(iv_col) > 0, m3 / (get(iv_col)^(3/2)), NA_real_)]

  # 4) Good minus Bad variance (GMB)
  dt[, IVp := frollmean((get(resid_col)^2) * (get(resid_col) > 0), n = window), by = symbol]
  dt[, IVm := frollmean((get(resid_col)^2) * (get(resid_col) < 0), n = window), by = symbol]
  dt[, (gmb_col) := fifelse(get(iv_col) > 0, (IVp - IVm) / get(iv_col), NA_real_)]

  # Clean up temps
  dt[, c("b0","b_mkt","b_mom","m3","IVp","IVm") := NULL]
  setorder(dt, symbol, date)
  dt
}
iv(prices)
iv(prices, 66)

# # Rolling idiosyncratic
# setorder(prices, symbol, date)
# prices[, sd_roll_year_idio := roll::roll_sd(resid, width = 252), by = symbol]
# prices[, sd_roll_halfyear_idio := roll::roll_sd(resid, width = 125), by = symbol]
# prices[, sd_roll_month_idio := roll::roll_sd(resid, width = 22), by = symbol]
# weights_ = c(252, 150, 22) / sum(c(252, 150, 22))
# prices[, sd_average_year_idio :=
#          sd_roll_year_idio * weights_[3] +
#          sd_roll_halfyear_idio * weights_[2] +
#          sd_roll_month_idio * weights_[1]
# ]

# # Tail risk
# prices[, resid_neg := fifelse(resid < 0, resid, 0)]
# prices[, sd_roll_year_neg := roll_sd(resid_neg, width = 252), by = symbol]
# prices[, sd_roll_halfyear_neg := roll_sd(resid_neg, width = 125), by = symbol]
# prices[, sd_roll_month_neg := roll_sd(resid_neg, width = 22), by = symbol]
# weights_ = c(252, 125, 22) / sum(c(252, 125, 22))
# prices[, sd_average_year_neg :=
#          sd_roll_year_neg * weights_[3] +
#          sd_roll_halfyear_neg * weights_[2] +
#          sd_roll_month_neg * weights_[1]
# ]

# Lottery (MAX): max 1d return over last 21d
prices[, max21 := roll::roll_max(returns, 21), by = symbol]

# ADV20 in USD
prices[, dollar_vol := close_raw * volume]
prices[, adv20 := frollmean(dollar_vol, 22), by = symbol]

# # Basic cleaning
# dt = prices[, .(symbol, date, open, close, close_raw, volume, returns, spy,
#                 iwm, month, sd_average_year, beta_average_year,
#                 sd_average_year_idio, sd_average_year_neg, max21, mom)]
# dt = na.omit(dt,
#              cols = c("sd_average_year", "beta_average_year",
#                       "sd_average_year_idio", "sd_average_year_neg", "mom"))


# UNIVERSE ----------------------------------------------------------------
# Coarse universe filtering
# 1) etf
dt = prices[spy == 1]
# 1) dv
dt = prices[close_raw > 2 & adv20 > 1e7]

# Cap very big or very low values
nrow(dt[beta_average_year > 4]) / nrow(dt) * 100
nrow(dt[beta_average_year < -4]) / nrow(dt) * 100
hist(dt[, beta_average_year])
dt = dt[beta_average_year %between% c(-4, 4)]
dt[, median(sd_average_year)]
nrow(dt[sd_average_year > 4]) / nrow(dt) * 100
nrow(dt[sd_average_year < 0.0005]) / nrow(dt) * 100
dt = dt[sd_average_year %between% c(0.005, 4)]
# dt[, median(sd_average_year_idio)]
# nrow(dt[sd_average_year_idio > 4]) / nrow(dt) * 100
# nrow(dt[sd_average_year_idio < 0.0005]) / nrow(dt) * 100
# dt = dt[sd_average_year_idio %between% c(0.005, 4)]

# Remove NA values
dt = na.omit(
  dt,
  cols = c("sd_average_year", "beta_average_year", "mom", "IV_66")
  )

# Descriptive
summary(dt[, sd_average_year])
summary(dt[, beta_average_year])
summary(dt[, GMB_22])
summary(dt[, GMB_66])
cor(dt[, .(sd_average_year, beta_average_year, GMB_22)])
dt[, .(sd_average_year, beta_average_year, GMB_22)]
dt[, cor(sd_average_year, GMB_22)]
dt[, .N, by = date]


# RANKS -------------------------------------------------------------------
# Define all ranks
dt[, beta_rank := ((frank(beta_average_year, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, sd_rank := ((frank(sd_average_year, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
# dt[, sd_rank_idio := ((frank(sd_average_year_idio, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
# dt[, sd_rank_neg := ((frank(sd_average_year_neg, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, max21_rank    := ((frank(max21, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, mom_rank      := ((frank(mom, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, iv_rank_22    := ((frank(IV_22, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, iv_rank_66    := ((frank(IV_66, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, iskew_rank_22 := ((frank(ISKEW_22, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, iskew_rank_66 := ((frank(ISKEW_66, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, gmb_rank_22   := ((frank(GMB_22, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, gmb_rank_66   := ((frank(GMB_66, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]

# Combine ranks
dt[, rank := beta_rank + sd_rank]
# dt[, rank_idio := beta_rank + sd_rank_idio]
# dt[, rank_neg := beta_rank + sd_rank_neg]
dt[, rank_bundle_22 := beta_rank + iv_rank_22]
dt[, rank_bundle_66 := beta_rank + iv_rank_66]
dt[, rank_bundle_22 := beta_rank + iv_rank_66]

# Target variable
setorder(dt, symbol, date)
dt[, target := shift(returns, 1, type = "lead"), by = symbol]
dt = na.omit(dt, cols = "target")


# PERFORMANCE -------------------------------------------------------------
# Returns over bins
plot_ret_over_rank = function(r) {
  dt[, .(ret = mean(target, na.rm = TRUE)), .(bin = dplyr::ntile(x, 20)), env = list(x = r)] |>
    _[order(ret)] |>
    ggplot(aes(bin, ret)) +
    geom_bar(stat = "identity")
}
plot_ret_over_rank("rank")
# plot_ret_over_rank("rank_idio")
# plot_ret_over_rank("rank_neg")
plot_ret_over_rank("rank_bundle_22")
plot_ret_over_rank("rank_bundle_66")

plot_monthly_bin_retutrns = function(r) {
  dt[, .(
    open = data.table::first(open),
    close = data.table::last(close),
    x = data.table::last(x)
  ), by = .(symbol, month), env = list(x = r)] |>
    _[, target := close / open - 1] |>
    _[, target := shift(target, 1, type = "lead")] |>
    _[, .(ret = mean(target, na.rm = TRUE)), .(bin = dplyr::ntile(x, 20)), env = list(x = r)] |>
    _[order(ret)] |>
    ggplot(aes(bin, ret)) +
    geom_bar(stat = "identity")
}
plot_monthly_bin_retutrns("rank")
# plot_monthly_bin_retutrns("rank_idio")
# plot_monthly_bin_retutrns("rank_neg")
plot_monthly_bin_retutrns("rank_bundle_22")
plot_monthly_bin_retutrns("rank_bundle_66")

# dt[, .(
#   open = data.table::first(open),
#   close = data.table::last(close),
#   rank = data.table::last(rank)
# ), by = .(symbol, month)] |>
#   _[, target := close / open - 1] |>
#   _[, target := shift(target, 1, type = "lead")] |>
#   _[, bin := dplyr::ntile(rank, 10)] |>
#   ggplot(aes(bin, target)) +
#   ggbeeswarm::geom_quasirandom(
#     alpha = 0.8, size = 2,
#     aes(fill = bin)
#   )

# Daily rebalancing
setorder(dt, date, rank)
# Long only
back = dt[, head(.SD, 30), by = date]
back[, weight := 1/length(target), by = date]
back = back[, .(ret = sum(target * weight, na.rm = TRUE)), by = date]
setorder(back, date)
back = as.xts.data.table(back)
charts.PerformanceSummary(back)
SharpeRatio.annualized(back)
# SharpeRatio.annualized(back["2019/"])
# charts.PerformanceSummary(back["2019/"])
# Short
back = dt[rank < 150][, tail(.SD, 10), by = date]
back[, weight := 1/length(target), by = date]
back = back[, .(ret = sum(target * weight, na.rm = TRUE)), by = date]
setorder(back, date)
back = as.xts.data.table(back)
back = back[back < 1]
charts.PerformanceSummary(back)
SharpeRatio.annualized(back)

# Montlhy backtest function
backtestm = function(r, n = 50, sign = 1, filter = FALSE) {
  back = dt[, .(
    open = data.table::first(open),
    close = data.table::last(close),
    GMB   = data.table::last(GMB_66),
    x = data.table::last(x)
  ), by = .(symbol, month), env = list(x = r)]
  if (filter == TRUE) {
    back = back[shift(GMB) > 0]
  }
  # plot(back[, .N, by = month][, N])
  back[, target := close / open - 1]
  back[, target := shift(target, 1, type = "lead")]
  setorderv(back, c("month", r))
  if (sign == 1) {
    back = back[, head(.SD, n), by = month]
  } else {
    back = back[, tail(.SD, n), by = month]
  }
  back[, weight := 1/length(target), by = month]
  back = back[, .(ret = sum(target * weight * sign, na.rm = TRUE)), by = month]
  back = as.xts.data.table(back[, .(zoo::as.Date.yearmon(month), ret)])
  return(back)
}

# Monthly rebalancing backteests for variaous ranks
finutils::portfolio_stats(backtestm("rank"), scale = 12)
charts.PerformanceSummary(backtestm("rank"))
finutils::portfolio_stats(backtestm("rank", filter = TRUE), scale = 12)
charts.PerformanceSummary(backtestm("rank", filter = TRUE))
# finutils::portfolio_stats(backtestm("rank_idio"), scale = 12)
# charts.PerformanceSummary(backtestm("rank_idio"))
# finutils::portfolio_stats(backtestm("rank_neg"), scale = 12)
# charts.PerformanceSummary(backtestm("rank_neg"))
finutils::portfolio_stats(backtestm("rank_bundle_22"), scale = 12)
charts.PerformanceSummary(backtestm("rank_bundle_22"))
finutils::portfolio_stats(backtestm("rank_bundle_66"), scale = 12)
charts.PerformanceSummary(backtestm("rank_bundle_66"))
finutils::portfolio_stats(backtestm("rank_bundle_66", filter = TRUE), scale = 12)
charts.PerformanceSummary(backtestm("rank_bundle_66", filter = TRUE))
# finutils::portfolio_stats(backtestm("rank_bundle_2"), scale = 12)
# charts.PerformanceSummary(backtestm("rank_bundle_2"))
# finutils::portfolio_stats(backtestm("rank_bundle_3"), scale = 12)
# charts.PerformanceSummary(backtestm("rank_bundle_3"))

# Shorts
finutils::portfolio_stats(backtestm("rank", sign = -1), scale = 12)
charts.PerformanceSummary(backtestm("rank", sign = -1))
finutils::portfolio_stats(backtestm("rank_idio", sign = -1), scale = 12)
charts.PerformanceSummary(backtestm("rank_idio", sign = -1))
finutils::portfolio_stats(backtestm("rank_neg", sign = -1), scale = 12)
charts.PerformanceSummary(backtestm("rank_neg", sign = -1))
finutils::portfolio_stats(backtestm("rank_bundle", sign = -1), scale = 12)
charts.PerformanceSummary(backtestm("rank_bundle", sign = -1))
finutils::portfolio_stats(backtestm("rank_bundle_2", sign = -1), scale = 12)
charts.PerformanceSummary(backtestm("rank_bundle_2", sign = -1))
finutils::portfolio_stats(backtestm("rank_bundle_3", sign = -1), scale = 12)
charts.PerformanceSummary(backtestm("rank_bundle_3", sign = -1))

# Long Short
back = dt[, .(
  open = data.table::first(open),
  close = data.table::last(close),
  rank = data.table::last(rank)
), by = .(symbol, month)]
back[, target := close / open - 1]
back[, target := shift(target, 1, type = "lead")]
setorder(back, month, rank)
back_short = back[, tail(.SD, 30), by = month]
back_short = back_short[, weight := -1 / length(target) * 0.5, by = month]
back_long  = back[, head(.SD, 30), by = month]
back_long  = back_long[, weight := 1 / length(target), by = month]
back = rbind(back_short, back_long)
back = back[, .(ret = sum(target * weight, na.rm = TRUE)), by = month]
back = as.xts.data.table(back[, .(zoo::as.Date.yearmon(month), ret)])
charts.PerformanceSummary(back)
SharpeRatio.annualized(back)
# SharpeRatio.annualized(back["2019/"])
# charts.PerformanceSummary(back["2019/"])

