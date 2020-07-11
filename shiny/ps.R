source('utils.R')

#' Reading in ps (server jobs) log 
ps_read = function(conn, table='ps', time_val=4, units='hours', host_name='rick'){
  X = median(1:(time_val-3))
  sql = sql_recent(table=table, time_val=time_val, units=units)
  df = DBI::dbGetQuery(conn, sql) %>%
      filter(hostname == !!host_name) %>%
      mutate(time = format_time(time),
             time_rank = time %>% as.factor %>% as.numeric) %>%
      filter(time_rank %% (time_val - 3) %in% c(0,X)) %>%
      select(-time_rank) %>%
      group_by(time, uname) %>%
      summarize(n_jobs = n(),
                perc_cpu = sum(as.numeric(perc_cpu)),
                perc_mem = sum(as.numeric(perc_mem))) %>%
      ungroup()
  return(df)
}

