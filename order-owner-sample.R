library(data.table)

options(max.print=2000)
rm(list=ls())

setwd('~/Data/github')

dt <- fread('owner-repos.csv')
dt[,repos := NULL]

set.seed(1)
dt[,selector := runif(.N)]
dt <- dt[order(selector)]

write.table(dt[,owner],
            file='owner-sample-order.csv',
            row.names=FALSE,
            col.names=FALSE,
            quote=FALSE)



