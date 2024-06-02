#install.packages("RPostgres","DBI","readxl")
library(RPostgres)
library(DBI)
library(readxl)

final_sdtmcon<-function(excel_path="",sheet_name="",sql_table_name=""){
  
# Read Excel sheet into a data frame
  
excel_df_sdtm<- read_excel(excel_path,sheet = sheet_name)

# Setting up the PostgreSQL connection
# con <- dbConnect(
#     RPostgres::Postgres(),
#     dbname = "sdtm_configs ",
#     host = "localhost",
#     port = "5432",
#     user = "postgres",
#     password = "password"
#   )
# Set up the PostgreSQL connection
con <- dbConnect(RPostgres::Postgres(),
                 dbname = "ipvdev1_i0g0",
                 host = "dpg-cn3lhf5jm4es73blmlj0-a.oregon-postgres.render.com",
                 port = "5432",
                 user = "sdtm",
                 password = "sdtm@1234"
)
  
  # Reading sqldata into an R data frame
sql_query <- paste("SELECT * FROM", sql_table_name)
sql_df_sdtm <- dbGetQuery(con, sql_query)
  
  if(sheet_name=="Datasets"){

    #merging two data frames
    merged_sdtm <- merge(sql_df_sdtm, excel_df_sdtm,
                         by.x = c('sdtm_version', 'class', 'dataset_name', 'dataset_label', 'structure'),
                         by.y = c( "Version","Class","Dataset Name","Dataset Label","Structure") , all = TRUE,row.names=FALSE)
    
  }
  else {
    #merging two data frames
    merged_sdtm <- merge(sql_df_sdtm, excel_df_sdtm,
                            by.x = c('sdtm_version', 'variable_order', 'class', 'dataset_name', 'variable_name', 'variable_label',
                                     'variable_type', 'described_value_domain', 'role', 'variables_qualified', 'usage_restrictions', 
                                     'variable_c_code', 'definition', 'notes', 'examples'),
                            by.y = c(  "Version","Variable Order","Class","Dataset Name","Variable Name","Variable Label",
                                       "Type","Described Value Domain","Role","Variables Qualified","Usage Restrictions","Variable C-Code",       
                                       "Definition","Notes","Examples" ) , all = TRUE,row.names=FALSE)
    
  }
  
  dbWriteTable(con,sql_table_name, merged_sdtm,overwrite = TRUE,row.names=FALSE)
  
  # Close the PostgreSQL connection 
  dbDisconnect(con)
  

  
}

final_sdtmcon("C:/Users/Adhitya/Downloads/SDTM_v1.7.xlsx","Datasets","sdtm_datasets")

rm(list=ls())
