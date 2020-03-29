source('utils.R')

#' Reading in ps (server jobs) log 
ps_read = function(conn, table='ps', time_val=4, units='hours', host_name='rick'){
  sql = sql_recent(table=table, time_val=time_val, units=units)
  df = DBI::dbGetQuery(conn, sql) %>%
      filter(hostname == !!host_name) %>%
      group_by(time, uname) %>%
      summarize(n_jobs = n(),
                perc_cpu = sum(as.numeric(perc_cpu)),
                perc_mem = sum(as.numeric(perc_mem))) %>%
      ungroup()
  return(df)
}

