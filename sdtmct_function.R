#install.packages("RPostgres","DBI","readxl")
library(RPostgres)
library(DBI)
library(readxl)


final_sdtmcon<-function(excel_path="",sheet_name="",sql_table_name=""){
  
  # Read Excel sheet into a data frame
  
  excel_df_ct<- read_excel(excel_path,sheet = sheet_name)
  
  # Setting up the PostgreSQL connection
  # con <- dbConnect(
  #   RPostgres::Postgres(),
  #   dbname = "sdtm_configs ",
  #   host = "localhost",
  #   port = "5432",
  #   user = "postgres",
  #   password = "password"
  # )
  # 
  
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
  sql_df_ct <- dbGetQuery(con, sql_query) 
  
  #merging two data frames
  merged_ct <- merge(sql_df_ct, excel_df_ct,
                     by.x = c('define_standards_id','codelist_code', 'codelist_extensible', 'codelist_name', 'cdisc_submission_value', 
                              'cdisc_synonyms', 'cdisc_definition', 'nci_preferred_term', 'standard_and_date'),
                     by.y = c( "Code","Codelist Code","Codelist Extensible (Yes/No)","Codelist Name","CDISC Submission Value",
                               "CDISC Synonym(s)","CDISC Definition","NCI Preferred Term","Standard and Date") , all = TRUE,row.names=FALSE)

  
  dbWriteTable(con,sql_table_name, merged_ct,overwrite = TRUE,row.names=FALSE) 
  
  # Close the PostgreSQL connection 
  dbDisconnect(con)
  
  return(merged_ct)
  
}

final_sdtmcon("C:/Users/Adhitya/Downloads/SDTM_CT_2023-12-15 (1).xlsx","Terminology","sdtm_ct")

rm(list=ls())
