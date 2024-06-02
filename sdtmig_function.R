#install.packages("RPostgres","DBI","readxl")
library(RPostgres)
library(DBI)
library(readxl)

final_sdtmcon<-function(excel_path="",sheet_name="",sql_table_name=""){

# Read Excel sheet into a data frame

excel_df<- read_excel(excel_path,sheet = sheet_name)

# Setting up the PostgreSQL connection
# con <- dbConnect(
#   RPostgres::Postgres(),
#   dbname = "sdtm_configs ",
#   host = "localhost",
#   port = "5432",
#   user = "postgres",
#   password = "password"
# )

con <- dbConnect(RPostgres::Postgres(),
                 dbname = "ipvdev1_i0g0",
                 host = "dpg-cn3lhf5jm4es73blmlj0-a.oregon-postgres.render.com",
                 port = "5432",
                 user = "sdtm",
                 password = "sdtm@1234"
)


# Reading sqldata into an R data frame
sql_query <- paste("SELECT * FROM", sql_table_name)
sql_df <- dbGetQuery(con, sql_query)

if(sheet_name=="Datasets"){
#merging two data frames
merged_df_sdtmig<- merge(sql_df, excel_df, 
                         by.x = c('sdtmig_version','class','dataset_name','dataset_label','structure'),
                         by.y = c('Version','Class','Dataset Name','Dataset Label','Structure'),
                         all = TRUE,row.names=FALSE)
}
else {
#merging two data frames
merged_df_sdtmig <- merge(sql_df, excel_df,
                            by.x = c('sdtmig_version', 'variable_order', 'class', 'dataset_name', 'variable_name', 'variable_label', 'variable_type', 
                                     'ct_code_list_code', 'codelist_submission_value', 'described_value_domain', 'value_list', 'role', 'cdisc_notes','core'),
                            by.y = c("Version","Variable Order","Class","Dataset Name","Variable Name","Variable Label","Type","CDISC CT Codelist Code(s)",
                                     "Codelist Submission Value(s)","Described Value Domain(s)","Value List","Role","CDISC Notes","Core" ), all = TRUE,row.names=FALSE)

}

dbWriteTable(con,sql_table_name, merged_df_sdtmig,overwrite = TRUE,row.names=FALSE)

# Close the PostgreSQL connection 
dbDisconnect(con)


}

final_sdtmcon("C:/Users/Adhitya/Downloads/SDTMIG-MD_v1.1.xlsx","Datasets","sdtmig_datasets")

rm(list=ls())
