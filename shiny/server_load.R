source('utils.R')

#' Reading in server load log 
server_load_read = function(conn, table='server_load', time_val=4, units='hours'){
  sql = sql_recent(table=table, time_val=time_val, units=units)
  df = DBI::dbGetQuery(conn, sql) %>%
      mutate(time = as.POSIXct(time, tz=Sys.timezone()), # strptime(time, "%Y-%m-%d %H:%M:%S"),
             io_load = as.numeric(io_load)) %>%
      filter(!is.na(time),
             !is.na(io_load))
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
