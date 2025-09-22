library(data.table)
library(lubridate)

# Globals
if (interactive()) {
  PATH = "D:/strategies/lowvol"
}

# Create indicators directory if it doesn't exist
if (interactive()) {
  if (!dir.exists(file.path(PATH, "indicators"))) {
    dir.create(file.path(PATH, "indicators"))
  }
}

# Import data and select columns we need
files = list.files(file.path(PATH, "predictors_daily"), full.names = TRUE)
files = files[1:5]

dtd = lapply(files, fread)

# Import predictors in chunks Predictors
cols = c("symbol", "date", colnames(fread(files[1]))[grepl("^d_f", colnames(fread(files[1])))])
dtd = lapply(files, fread, select = cols)
dtd = rbindlist(dtd, fill= TRUE)
setorder(dtd, symbol, date)
keep = dtd[, vapply(.SD, function(x) (sum(is.na(x)) / length(x)) < 0.5, logical(1L))]
keep = names(keep[keep == TRUE])
dtd = dtd[, ..keep]

# Descriptive
dtd[, hist(d_fdGPH_0_5_252_log)]
dtd[, hist(d_fdGPH_0_9_252_log)]

#



library(fracdiff)


ts.test <- fracdiff.sim( 5000, ar = .2, ma = -.4, d = .3)
fd. <- fracdiff( ts.test$series,
                 nar = length(ts.test$ar), nma = length(ts.test$ma))
fd.
## Confidence intervals
confint(fd.)

## with iteration output
fd2 <- fracdiff(ts.test$series, nar = 1, nma = 1, trace = 1)
all.equal(fd., fd2)

head(diffseries(ts.test$series, 0.3))
head(fd.$fitted)
head(fd.$residuals)
fd.$
