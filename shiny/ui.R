library(shiny)
library(plotly)


#shinyUI(fluidPage(
shinyUI(
  navbarPage("Compute status",
             tabPanel("Server load",
                      fluidRow(
                        column(1),
                        column(3, selectInput('time_unit', 
                                               label = 'Time unit',
                                               choices = c('days', 'hours', 'minutes'),
                                               selected = 'hours')),
                        column(5, numericInput('time_val', 
                                               label = 'Most recent {Time unit} to display',
                                               value = 4,
                                               min = 1,
                                               max = 24 * 7 * 4,
                                               step = 1)),
                        column(8)
                      ),
                      plotOutput("server_load_plot_obj", height='500px')
             ),
             tabPanel("Server/cluster jobs",
                      fluidRow(
                        column(2, numericInput('min_num_jobs',
                                               label = 'Job count cutoff',
                                               value = 5,
                                               min = 1,
                                               step = 1)),
                        column(3, textInput('uname',
                                            label = 'Username filter',
                                            value = NULL)),
                        column(3, selectInput('time_unit2', 
                                              label = 'Time unit',
                                              choices = c('days', 'hours', 'minutes'),
                                              selected = 'hours')),
                        column(4, numericInput('time_val2', 
                                               label = 'Most recent {Time unit} to display',
                                               value = 4,
                                               min = 1,
                                               max = 24 * 7 * 4,
                                               step = 1))
                      ),
                      tabsetPanel(type = "tabs",
                                  tabPanel('Cluster', 
                                           plotlyOutput("qstat_plot_obj", height='600px')),
                                  tabPanel('rick VM',
                                           plotlyOutput("ps_rick_plot_obj", height='600px')),
                                  tabPanel('morty VM',
                                           plotlyOutput("ps_morty_plot_obj", height='600px'))
                      )
             ),
             tabPanel("Project sizes",
                      fluidRow(
                        column(3, textInput('dirname_disk',
                                            label = 'Directory name filter',
                                            value = NULL))
                      ),
                      tabsetPanel(type = "tabs",
                                  tabPanel('abt3-projects', 
                                           plotlyOutput("du_plot_abt3_projects",
                                                        height='1050px')),
                                  tabPanel('abt3-scratch', 
                                           plotlyOutput("du_plot_abt3_scratch",
                                                        height='600px')),
                                  #tabPanel('tmp-global', 
                                  #         plotlyOutput("du_plot_tmp_global",
                                  #                      height='600px')),
                                  tabPanel('tmp-global2', 
                                           plotlyOutput("du_plot_tmp_global2",
                                                        height='600px')),
                                  tabPanel('abt3-home', 
                                           plotlyOutput("du_plot_abt3_home",
                                                        height='500px'))
                      )
             ),
             tabPanel("Project inodes",
                      fluidRow(
                        column(3, textInput('dirname_inodes',
                                            label = 'Directory name filter',
                                            value = NULL))
                      ),
                      tabsetPanel(type = "tabs",
                                  tabPanel('abt3-projects', 
                                           plotlyOutput("inodes_plot_abt3_projects",
                                                        height='1050px')),
                                  tabPanel('abt3-scratch', 
                                           plotlyOutput("inodes_plot_abt3_scratch",
                                                        height='600px')),
                                  #tabPanel('tmp-global', 
                                  #         plotlyOutput("inodes_plot_tmp_global",
                                  #                      height='600px')),
                                  tabPanel('tmp-global2', 
                                           plotlyOutput("inodes_plot_tmp_global2",
                                                        height='600px')),
                                  tabPanel('abt3-home', 
                                           plotlyOutput("inodes_plot_abt3_home",
                                                        height='500px'))
                      )
             ),
             tabPanel("RStudio Server Dashboard",
                      fluidRow(
                        br(),
                        column(12, htmlOutput('rstudo_dashboard'))
                      ),
                      fluidRow(
                        column(6,
                               h5('Direct link: ', tags$a(href="http://morty.eb.local:8787/admin/dashboard", "RStudio Server Dashboard"))
                        )
                      )
             )
  ))