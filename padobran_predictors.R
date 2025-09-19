library(data.table)
library(finfeatures)


# paths
if (interactive()) {
  PATH_PRICES     = file.path("D:/strategies/lowvol/prices")
  PATH_PREDICTORS = file.path("D:/strategies/lowvol/predictors_daily")
} else {
  PATH_PRICES = file.path("prices")
  PATH_PREDICTORS = file.path("predictors_daily")
}

# Create directory if it doesnt exists
if (!dir.exists(PATH_PREDICTORS)) {
  dir.create(PATH_PREDICTORS)
}

# Get index
if (interactive()) {
  i = 1L
} else {
  i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
}

# Get symbol
symbols = gsub("\\.csv", "", list.files(PATH_PRICES))
symbol_i = symbols[i]

# Import Ohlcv data
ohlcv = fread(file.path(PATH_PRICES, paste0(symbol_i, ".csv")))
head(ohlcv, 20)
ohlcv = Ohlcv$new(ohlcv[, .(symbol, date, open, high, low, close, volume)],
                  date_col = "date")

# Exuber
fracdiff_init = RollingFracdiff$new(
  windows = c(252),
  workers = 1L,
  at = 1:nrow(ohlcv$X),
  lag = 0L,
  nar = c(1),
  nma = c(1)
)
fracdiff_p = fracdiff_init$get_rolling_features(ohlcv, log_prices = TRUE)
fwrite(fracdiff_p, file.path(PATH_PREDICTORS, paste0(symbol_i, ".csv")))

# Delete prices folder to free space
