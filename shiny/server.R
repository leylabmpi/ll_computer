library(shiny)
library(dplyr)
library(tidyr)
library(ggplot2)
library(dbplyr)
library(plotly)
library(DBI)


source('utils.R')
source('server_load.R')
source('qstat.R')
source('ps.R')
source('usage.R')
#source('node_FS_mount.R')


shinyServer(function(input, output, session){
  cancel.onSessionEnded <- session$onSessionEnded(function() {
    dbDisconnect(conn)
    cat("Connection to database terminated\n")
  })
  
  # database connect
  db_file = '/ebio/abt3_projects2/databases_no-backup/ll_computer/ll_computer.db'
  conn = dbConnect(RSQLite::SQLite(),
                   dbname = db_file)
  
  # setting timers for reload data
  second_timer = reactiveTimer(intervalMs = 1000, session = session)
  minute_timer = reactiveTimer(intervalMs = 1000 * 60, session = session)
  minute5_timer = reactiveTimer(intervalMs = 1000 * 60 * 5, session = session)
  minute30_timer = reactiveTimer(intervalMs = 1000 * 60 * 30, session = session)
  
  #-- reactive --#
  # server load
  observe({
    minute_timer()
      output$server_load_plot_obj <- renderPlot({
        server_load_plot(server_load_read(conn, 
                                          time_val=input$time_val, 
                                          units=input$time_unit), 
                         input)
      })
  })
  # qstat + ps
  observe({
    minute5_timer()
    output$qstat_plot_obj <- renderPlotly({
      qstat_plot(qstat_read(conn, time_val=input$time_val2, units=input$time_unit2), 
                 input)
    })
    output$ps_rick_plot_obj <- renderPlotly({
      qstat_plot(ps_read(conn, time_val=input$time_val2, units=input$time_unit2,
                         host_name='rick'), 
                 input)
    })
    output$ps_morty_plot_obj <- renderPlotly({
      qstat_plot(ps_read(conn, time_val=input$time_val2, 
                         units=input$time_unit2,
                         host_name='morty'), 
                 input)
    })
  })
  # disk/io usage
  observe({
    minute30_timer()
    # disk usage
    output$du_plot_abt3_projects <- renderPlotly({
      disk_usage_plot(disk_usage_read(conn, data_type='disk usage', 
                                      filesystem='abt3-projects',
                                      project_sizes='project_sizes',
                                      time_val=2, units='days'), 
                      input)
    })
    output$du_plot_abt3_scratch <- renderPlotly({
      disk_usage_plot(disk_usage_read(conn, data_type='disk usage', 
                                      filesystem='abt3-scratch',
                                      time_val=2, units='days'), 
                      input)
    })
    output$du_plot_tmp_global <- renderPlotly({
      disk_usage_plot(disk_usage_read(conn, data_type='disk usage', 
                                      filesystem='tmp-global',
                                      time_val=2, units='days'), 
                      input)
    })
    output$du_plot_tmp_global2 <- renderPlotly({
      disk_usage_plot(disk_usage_read(conn, data_type='disk usage', 
                                      filesystem='tmp-global2',
                                      time_val=2, units='days'), 
                      input)
    })
    output$du_plot_abt3_home <- renderPlotly({
      disk_usage_plot(disk_usage_read(conn, data_type='disk usage', 
                                      filesystem='abt3-home',
                                      time_val=2, units='days'), 
                      input)
    })
    # I/O usage
    output$inodes_plot_abt3_projects <- renderPlotly({
      disk_usage_plot(disk_usage_read(conn, data_type='inodes', 
                                      filesystem='abt3-projects',
                                      project_sizes='project_sizes',
                                      time_val=2, units='days'), 
                      input)
    })
    output$inodes_plot_abt3_scratch <- renderPlotly({
      disk_usage_plot(disk_usage_read(conn, data_type='inodes', 
                                      filesystem='abt3-scratch',
                                      time_val=2, units='days'), 
                      input)
    })
    output$inodes_plot_tmp_global <- renderPlotly({
      disk_usage_plot(disk_usage_read(conn, data_type='inodes', 
                                      filesystem='tmp-global',
                                      time_val=2, units='days'), 
                      input)
    })
    output$inodes_plot_tmp_global2 <- renderPlotly({
      disk_usage_plot(disk_usage_read(conn, data_type='inodes', 
                                      filesystem='tmp-global2',
                                      time_val=2, units='days'), 
                      input)
    })
    output$inodes_plot_abt3_home <- renderPlotly({
      disk_usage_plot(disk_usage_read(conn, data_type='inodes', 
                                      filesystem='abt3-home',
                                      time_val=2, units='days'), 
                      input)
    })
  })
})
