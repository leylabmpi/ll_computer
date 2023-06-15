#' Reading in disk-usage log
#' data_type = c('disk usage', 'inodes')
#' disk_usage = c('tmp-global', 'tmp-global2', 'abt3-projects', 'abt3-home', 'abt3-scratch')
disk_usage_read = function(conn, table='disk_usage', data_type='disk usage', 
                           filesystem='abt3-projects', project_sizes=NULL,
                           time_val=2, units='days'){
  # getting disk usage info
  sql = sql_recent(table=table, time_val=time_val, units=units, 
                   data_type=data_type, filesystem=filesystem)
  df = DBI::dbGetQuery(conn, sql) %>%
      dplyr::select(-data_type, -filesystem) %>%
      mutate(time = sapply(time, format_time))
  if(data_type == 'inodes'){
    df = df %>% 
      rename('million_files' = terabytes) %>%
      mutate(million_files = million_files / 1000.0)
      #mutate(million_files = 0.23)
  } else {
    df = df %>% mutate(terabytes = terabytes / 1000.0)   # wrong unit in the database
  }
  # getting project sizes if not null
  if (!is.null(project_sizes)){
    sql = sql_recent(table=project_sizes, time_val=time_val, units=units, 
                     data_type=NULL, filesystem=NULL)
    df_sizes = DBI::dbGetQuery(conn, sql) %>%
      dplyr::select(-time) %>%
      distinct(directory, .keep_all=TRUE) %>%
      rename('max_size_Tb' = terabytes) 
    # joining
    df = df %>%
        inner_join(df_sizes, c('directory')) 
    # perc of max limit
    if(data_type == 'disk usage'){
      df = df %>%
        mutate(perc_of_max = terabytes / max_size_Tb * 100) 
    } else 
    if(data_type == 'inodes'){
      df = df %>%
        mutate(perc_of_max = million_files / (inodes / 1000) * 100)
    }
    df = df %>%
      dplyr::select(-max_size_Tb, -inodes)
  } else {
    df = df %>%
      mutate(perc_of_max = NA)
  }
  return(df)
}

disk_usage_plot = function(df, input){
  levs = c('Millions of files', 'Terabytes', '% of limit')
  if(nrow(df) < 1){
    return(NULL)
  }
  p = df %>%
    filter(max(time) - time < 5) %>%
    mutate(percent = percent %>% as.Num,
           directory = directory %>% as.character,
           directory = reorder(directory, percent)) %>%
    gather(unit, usage, -time, -directory, -perc_of_max) %>%
    mutate(unit = case_when(unit == 'million_files' ~ 'Millions of files',
                            unit == 'terabytes' ~ 'Terabytes',
                            unit == 'percent' ~ '% of limit',
                            TRUE ~ unit), 
           unit = factor(unit, levels=levs),
           usage = round(usage, 3),
           perc_of_max = round(perc_of_max, 3)) %>%
    rename(`% of limit` = perc_of_max) %>%
    ggplot(aes(directory, usage)) +
    coord_flip() +
    facet_grid(. ~ unit, scales='free_x') +
    theme_bw() +
    theme(
      axis.title.y = element_blank()
    )
  if(all(is.na(df$perc_of_max))){
    p = p + geom_bar(stat='identity')
  } else {
    cols = c('black', 'darkblue', 'purple', 'red', 'orange')
    p = p + 
      geom_bar(stat='identity', aes(fill=`% of limit`)) +
      scale_fill_gradientn('% of limit', colors=cols)
  }
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

