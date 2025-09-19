library(data.table)
library(finutils)


# Setup
PATH_PRICES = "D:/strategies/lowvol"
if (!dir.exists(PATH_PRICES)) dir.create(PATH_PRICES, recursive = TRUE)

# Prices data
prices = qc_daily_parquet(
  file_path = "F:/lean/data/all_stocks_daily",
  etfs = FALSE,
  min_obs = 252,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  profiles_fmp = TRUE,
  fmp_api_key = Sys.getenv("APIKEY")
)

# Remove ETF's
prices = prices[isEtf == FALSE & isFund == FALSE]

# Save every symbol separately
prices_dir = file.path(PATH_PRICES, "prices")
if (!dir.exists(prices_dir)) {
  dir.create(prices_dir)
}
for (s in prices[, unique(symbol)]) {
  if (s == "prn") next()
  prices_ = prices[symbol == s]
  if (nrow(prices_) < 22) next
  file_name = file.path(prices_dir, paste0(s, ".csv"))
  fwrite(prices_, file_name)
}

# Create sh file for predictors
cont = sprintf(
  "#!/bin/bash

#PBS -N predictors
#PBS -l ncpus=1
#PBS -l mem=1GB
#PBS -J 1-%d
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}

apptainer run image.sif padobran_predictors.R",
  length(list.files(prices_dir)))
writeLines(cont, "padobran_predictors.sh")

# Add to padobran
# scp -r /home/sn/data/strategies/pead/prices padobran:/home/jmaric/peadml/prices

