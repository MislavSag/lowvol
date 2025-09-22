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

# Setup
setCalendar("UnitedStates/NYSE")

# Prices data
prices = qc_daily_parquet(
  file_path = "F:/lean/data/all_stocks_daily",
  etfs = FALSE,
  etf_cons = c("F:/lean/data/equity/usa/universes/etf/iwm",
               "F:/lean/data/equity/usa/universes/etf/spy"),
  min_obs = 252,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  market_symbol = "spy",
  profiles_fmp = TRUE,
  fmp_api_key = Sys.getenv("FMP-SNP")
)
prices[, month := data.table::yearmon(date)]

# Remove ETFS
nrow(prices[isEtf == TRUE | isFund == TRUE]) / nrow(prices)
prices = prices[isEtf == FALSE & isFund == FALSE]

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

# Get residual returns
prices[, resid := returns - beta_average_year * spy_returns]

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

# Rolling idiosyncratic
setorder(prices, symbol, date)
prices[, sd_roll_year_idio := roll::roll_sd(resid, width = 252), by = symbol]
prices[, sd_roll_halfyear_idio := roll::roll_sd(resid, width = 125), by = symbol]
prices[, sd_roll_month_idio := roll::roll_sd(resid, width = 22), by = symbol]
weights_ = c(252, 150, 22) / sum(c(252, 150, 22))
prices[, sd_average_year_idio :=
         sd_roll_year_idio * weights_[3] +
         sd_roll_halfyear_idio * weights_[2] +
         sd_roll_month_idio * weights_[1]
]

# Tail risk
prices[, resid_neg := fifelse(resid < 0, resid, 0)]
prices[, sd_roll_year_neg := roll_sd(resid_neg, width = 252), by = symbol]
prices[, sd_roll_halfyear_neg := roll_sd(resid_neg, width = 125), by = symbol]
prices[, sd_roll_month_neg := roll_sd(resid_neg, width = 22), by = symbol]
weights_ = c(252, 125, 22) / sum(c(252, 125, 22))
prices[, sd_average_year_neg :=
         sd_roll_year_neg * weights_[3] +
         sd_roll_halfyear_neg * weights_[2] +
         sd_roll_month_neg * weights_[1]
]

# Lottery (MAX): max 1d return over last 21d
prices[, max21 := roll::roll_max(returns, 21), by = symbol]

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

# Basic cleaning
dt = prices[, .(symbol, date, open, close, close_raw, volume, returns, spy,
                iwm, month, sd_average_year, beta_average_year,
                sd_average_year_idio, sd_average_year_neg, max21, mom)]
dt = na.omit(dt,
             cols = c("sd_average_year", "beta_average_year",
                      "sd_average_year_idio", "sd_average_year_neg", "mom"))

# ADV20 in USD
dt[, dollar_vol := close_raw * volume]
dt[, adv20 := frollmean(dollar_vol, 22), by = symbol]

# Coarse universe filtering
# 1) etf
dt = prices[spy == 1]
# 1) dv
dt = dt[close_raw > 2 & adv20 > 1e7]

# Cap very big or very low values
nrow(dt[beta_average_year > 4]) / nrow(dt) * 100
nrow(dt[beta_average_year < -4]) / nrow(dt) * 100
dt = dt[beta_average_year %between% c(-4, 4)]
dt[, median(sd_average_year)]
nrow(dt[sd_average_year > 4]) / nrow(dt) * 100
nrow(dt[sd_average_year < 0.0005]) / nrow(dt) * 100
dt = dt[sd_average_year %between% c(0.005, 4)]
dt[, median(sd_average_year_idio)]
nrow(dt[sd_average_year_idio > 4]) / nrow(dt) * 100
nrow(dt[sd_average_year_idio < 0.0005]) / nrow(dt) * 100
dt = dt[sd_average_year_idio %between% c(0.005, 4)]

# Descriptive
summary(dt[, sd_average_year])
summary(dt[, beta_average_year])
dt[, cor(sd_average_year, beta_average_year)]
dt[, .N, by = date]

# Beta and vol ranks
dt[, beta_rank := ((frank(beta_average_year, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, sd_rank := ((frank(sd_average_year, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, sd_rank_idio := ((frank(sd_average_year_idio, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, sd_rank_neg := ((frank(sd_average_year_neg, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, max21_rank  := ((frank(max21, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, mom_rank    := ((frank(mom, ties.method = "min") - 1) / (.N - 1)) * 100,  by = date]
dt[, rank := beta_rank + sd_rank]
dt[, rank_idio := beta_rank + sd_rank_idio]
dt[, rank_neg := beta_rank + sd_rank_neg]
dt[, rank_bundle := beta_rank + sd_rank_idio + max21_rank]
dt[, rank_bundle_2 := beta_rank + sd_rank_idio + (1-mom_rank)]
dt[, rank_bundle_3 := ((beta_rank + sd_rank_idio) / 2) + (1-mom_rank)]

# Target variable
setorder(dt, symbol, date)
dt[, target := shift(returns, 1, type = "lead"), by = symbol]
dt = na.omit(dt, cols = "target")

# Returns over bins
plot_ret_over_rank = function(r) {
  dt[, .(ret = mean(target, na.rm = TRUE)), .(bin = dplyr::ntile(x, 20)), env = list(x = r)] |>
    _[order(ret)] |>
    ggplot(aes(bin, ret)) +
    geom_bar(stat = "identity")
}
plot_ret_over_rank("rank")
plot_ret_over_rank("rank_idio")
plot_ret_over_rank("rank_neg")
plot_ret_over_rank("rank_bundle")
plot_ret_over_rank("rank_bundle_2")
plot_ret_over_rank("rank_bundle_3")

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
plot_monthly_bin_retutrns("rank_idio")
plot_monthly_bin_retutrns("rank_neg")
plot_monthly_bin_retutrns("rank_bundle")
plot_monthly_bin_retutrns("rank_bundle_2")
plot_monthly_bin_retutrns("rank_bundle_3")

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
SharpeRatio.annualized(back["2019/"])
charts.PerformanceSummary(back["2019/"])
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
backtestm = function(r, n = 50, sign = 1) {
  back = dt[, .(
    open = data.table::first(open),
    close = data.table::last(close),
    x = data.table::last(x)
  ), by = .(symbol, month), env = list(x = r)]
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
finutils::portfolio_stats(backtestm("rank_idio"), scale = 12)
charts.PerformanceSummary(backtestm("rank_idio"))
finutils::portfolio_stats(backtestm("rank_neg"), scale = 12)
charts.PerformanceSummary(backtestm("rank_neg"))
finutils::portfolio_stats(backtestm("rank_bundle"), scale = 12)
charts.PerformanceSummary(backtestm("rank_bundle"))
finutils::portfolio_stats(backtestm("rank_bundle_2"), scale = 12)
charts.PerformanceSummary(backtestm("rank_bundle_2"))
finutils::portfolio_stats(backtestm("rank_bundle_3"), scale = 12)
charts.PerformanceSummary(backtestm("rank_bundle_3"))

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

