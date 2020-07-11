source('utils.R')

#' Reading in qstat log
qstat_read = function(conn, table='qstat', time_val=4, units='hours'){
  X = median(1:(time_val-3))
  sql = sql_recent(table=table, time_val=time_val, units=units)
  df = DBI::dbGetQuery(conn, sql) %>%
    rename('uname' = jb_owner) %>%
    mutate(time = format_time(time),
           time_rank = time %>% as.factor %>% as.numeric) %>%
    filter(time_rank %% (time_val - 3) %in% c(0,X)) %>%
    select(-time_rank) %>%
    group_by(time, uname) %>%
    summarize(n_jobs = n(),
              io_usage = sum(as.Num(io_usage), na.rm=TRUE),
              cpu_usage = sum(as.Num(cpu_usage), na.rm=TRUE),
              mem_usage = sum(as.Num(mem_usage), na.rm=TRUE)) %>%
    ungroup()
  return(df)
}

qstat_plot = function(df, input){ 
  # filtering
  df = df[df$n_jobs >= input$min_num_jobs,]
  if(! is.null(input$uname) & input$uname != ''){
    df = df[grepl(input$uname, df$uname),]
  }
  #df$time = format_time(df$time) 
  df = df[!is.na(df$time),]
  if(nrow(df) <= 0){
    return(NULL)
  }
  # plotting
  p = df %>%
    gather(Metric, Value, -time, -uname) %>%
    mutate(Metric = case_when(Metric == 'cpu_usage' ~ 'CPU usage',
                              Metric == 'perc_cpu' ~ 'CPU usage (%)',
                              Metric == 'io_usage' ~ 'I/O usage',
                              Metric == 'mem_usage' ~ 'Memory usage',
                              Metric == 'perc_mem' ~ 'Memory usage (%)',
                              Metric == 'n_jobs' ~ 'No. of jobs',
                              TRUE ~ Metric)) %>%
    ggplot(aes(time, Value, group=uname, color=uname)) +
    geom_line(alpha=0.25) +
    geom_point(alpha=0.5, size=0.7) +
    scale_color_discrete('Username') +
    facet_grid(Metric ~ ., scales='free_y') +
    theme_bw() +
    theme(
      axis.title.y = element_blank(),
      panel.spacing = unit(0.1, "lines")
    )
  return(p)
}
