source('utils.R')

#' Reading in server load log 
server_load_read = function(conn, table='server_load', time_val=4, units='hours'){
  X = ifelse(time_val-3 < 1, 1, time_val-3)
  X1 = round(quantile(1:X, 0.25),0)
  X2 = median(1:X)
  X3 = round(quantile(1:X, 0.75),0)
  sql = sql_recent(table=table, time_val=time_val, units=units)
  print(sql)
  df = DBI::dbGetQuery(conn, sql) %>%
      mutate(time = as.POSIXct(time, tz=Sys.timezone()), # strptime(time, "%Y-%m-%d %H:%M:%S"),
             io_load = as.numeric(io_load)) %>%
      filter(!is.na(time),
             !is.na(io_load)) %>%
      mutate(time_rank = time %>% as.factor %>% as.numeric) %>%
      filter(time_rank %% X %in% c(0,X1,X2,X3)) %>%
      select(-time_rank) 
  return(df)
}

server_load_plot = function(x, input){
  # plotting
  p = ggplot(x, aes(time, io_load, color=io_load)) + 
    geom_line() +
    geom_point() +
    scale_color_continuous(low='black', high='red') +
    labs(x='Date-Time', y='I/O load') +
    facet_grid(filesystem ~ ., scales='free_y') + 
    theme_bw() +
    theme(
      text = element_text(size=14),
      legend.position = 'none'
    )
  # return plot
  return(p)
}
