#' Reading in disk-usage log
#' data_type = c('disk usage', 'inodes')
#' disk_usage = c('tmp-global', 'tmp-global2', 'abt3-projects', 'abt3-home', 'abt3-scratch')
disk_usage_read = function(conn, table='disk_usage', 
                           data_type='disk usage', filesystem='abt3-projects', 
                           time_val=2, units='days'){
  sql = sql_recent(table=table, time_val=time_val, units=units, 
                   data_type=data_type, filesystem=filesystem)
  df = DBI::dbGetQuery(conn, sql) %>%
      dplyr::select(-data_type, -filesystem) %>%
      mutate(time = sapply(time, format_time))
  if(data_type == 'inodes'){
    df = df %>% rename('million_files' = terabytes)
  }
  return(df)
}

disk_usage_plot = function(df, input){
  levs = c('Millions of files', 'Terabytes', '% of total')
  if(nrow(df) < 1){
    return(NULL)
  }
  p = df %>%
    filter(max(time) - time < 5) %>%
    mutate(percent = percent %>% as.Num,
           directory = directory %>% as.character,
           directory = reorder(directory, percent)) %>%
    gather(unit, usage, -time, -directory) %>%
    mutate(unit = case_when(unit == 'million_files' ~ 'Millions of files',
                            unit == 'terabytes' ~ 'Terabytes',
                            unit == 'percent' ~ '% of total',
                            TRUE ~ unit), 
           unit = factor(unit, levels=levs)) %>%
    ggplot(aes(directory, usage)) +
    geom_bar(stat='identity') +
    coord_flip() +
    facet_grid(. ~ unit, scales='free_x') +
    theme_bw() +
    theme(
      axis.title.y = element_blank()
    )
  return(p)
}

inodes_log = function(file, input){
  x = fread(file, sep='\t', header=FALSE, fill=TRUE) %>%
    as.data.frame(x) %>%
    mutate(V4 = V4 / 1000)
  colnames(x) = c('Category', 'Time', 'Directory', 'Million_files', 'Percent')
  x$Time = as.POSIXct(strptime(x$Time, "%Y-%m-%d %H:%M:%S"))
  return(x)
}

