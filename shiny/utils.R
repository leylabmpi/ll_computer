format_time = function(x){
  x = as.POSIXct(as.POSIXlt(x,tz=Sys.timezone()))
  return(x)
}

as.Num = function(x){
  x = as.numeric(as.character(x))
  return(x)
}

#' sql for recent records in {table}
sql_recent = function(table, time_val=4, units='hours', 
                      data_type=NULL, filesystem=NULL){
  sql = "SELECT * FROM {table} WHERE time > DATETIME('now', 'localtime', '-{time} {units}')"
  sql = glue::glue(sql, table=table, time=time_val, units=units)
  if(!is.null(data_type)){
    sql = paste0(sql, glue::glue(" AND data_type == '{d}'", d=data_type))
  }
  if(!is.null(filesystem)){
    sql = paste0(sql, glue::glue(" AND filesystem == '{f}'", f=filesystem))
  }
  return(sql)
}

#' where are the log files
which_file = function(log_file){
  vol_dir = '/Volumes/abt3_projects/databases/server/'
  vm_dir = '/ebio/abt3_projects/databases/server/'
  F = file.path(vol_dir, log_file)
  if(! file.exists(F)){
    F = file.path(vm_dir, log_file)
  }
  return(F)
}