#### R script to quickly chart results of sqlite queries
####09/05/2016
####Jackson Whitmore


####Libraries
library(RSQLite)
library(ggplot2)
library(RColorBrewer)

####Connect to DB
con <- dbConnect(RSQLite::SQLite(), dbname = "MTA_Service.sqlite")

####Queries

#get count of statuses grouped by date, type
query1 <- dbGetQuery(con, "select Lines.Line_Name, Lines.Service_Type, Status.Stauts_Name, Date_Table.FullDate, Date_Table.DayNameShort, count(*) AS Count
From Status_Record 
JOIN Status on Status_Record.Status_ID = Status.Status_ID
JOIN Lines on Status_Record.Line_ID = Lines.Line_ID
JOIN Date_Table on Status_Record.Req_Date = Date_Table.DateKey
Group by Lines.Line_Name, Status.Stauts_Name, Date_Table.DateKey")
#format
query1[1:5] <- lapply(query1[1:5], as.factor)
query1$DayNameShort <- factor(query1$DayNameShort,
                              levels=c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"))

ggplot(subset(query1, Service_Type =="Subway"), 
       aes(x = DayNameShort, y = Count, fill = Line_Name)) +
  geom_bar(stat="identity", position = "dodge") + facet_wrap("Stauts_Name") +
  scale_fill_brewer(palette = "Set3")

#look into filling in zeros for lines that apparently haven't had a delay
#double check this is the case
#look into connecting directly to server


#####
#test out plotly