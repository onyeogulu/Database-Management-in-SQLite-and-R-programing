# ========================================================================================================================================================
# Read in the data stored in various sheets in the DataScience FoundationNEW excel file into different dataframe in R

install.packages('tidyverse')#install tidyvers package

library(readxl) # load in a library to load the excel file
HousePrice <- read_excel("DataScience FoundationNEW (1).xlsx", sheet = "HousePrice") # read in the sheet holding the House price data into the HousePrice dataframe
View(HousePrice) # view the HousePrice dataframe

DistrictTable <- read_excel("DataScience FoundationNEW (1).xlsx", sheet = "DistrictTable") # read in the sheet holding the district data into the DistrictTable dataframe
View(DistrictTable)# view the DistrictTable dataframe

Constituency <- read_excel("DataScience FoundationNEW (1).xlsx", sheet = "Constituency")# read in the sheet holding the Constituency data into the Constituency dataframe
View(Constituency)# view the Constituency dataframe

BroadBand <- read_excel("DataScience FoundationNEW (1).xlsx", sheet = "BroadBand")# read in the sheet holding the BroadBand data into the BroadBand dataframe
View(BroadBand)# view the BroadBand dataframe

TownsWard <- read_excel("DataScience FoundationNEW (1).xlsx", sheet = "TownsWard")# read in the sheet holding the TownsWard data into the TownsWard dataframe
View(TownsWard)# view the TownsWard dataframe

CouncilTaxData <- read_excel("DataScience FoundationNEW (1).xlsx", sheet = "CouncilTaxData")# read in the sheet holding the Constituency data into the DistrictTable dataframe
View(CouncilTaxData)# view the CouncilTaxData dataframe
# =============================================================================================================================================================================================================================
install.packages('RSQLite') # install the RSQLite package 
library(RSQLite)#Read in the library to enable connection to the SQLite database

con <- dbConnect(RSQLite::SQLite(),"DataScienceCoursework.db3" )#create a connection to the DataScienceCoursework SQLite database
dbListTables(con) # code to display the table in the DataScienceCoursework SQLite database

# ==============================================================================================================================================================================================================================
# Read dataframe into the various table in the DataScienceCoursework SQLite database
dbWriteTable(con, "HOUSEPRICE", HousePrice, append=TRUE) # code to populate the the HOUSEPRICE table in the DataScienceCoursework SQLite database with the HousePrice dataframe 

dbWriteTable(con, "BroadBand", BroadBand, append=TRUE)# code to populate the the BroadBand table in the DataScienceCoursework SQLite database with the BroadBand dataframe 

dbWriteTable(con, "Constituency", Constituency, append=TRUE)# code to populate the the Constituency table in the DataScienceCoursework SQLite database with the Constituency dataframe 

dbWriteTable(con, "CouncilTaxData", CouncilTaxData, append=TRUE)# code to populate the the CouncilTaxData table in the DataScienceCoursework SQLite database with the CouncilTaxData dataframe 

dbWriteTable(con, "DistrictTable", DistrictTable, append=TRUE)# code to populate the the DistrictTable table in the DataScienceCoursework SQLite database with the DistrictTable dataframe 

dbWriteTable(con, "TownsWard", TownsWard, append=TRUE)# code to populate the the TownsWard table in the DataScienceCoursework SQLite database with the TownsWard dataframe 

#=================================================================================================================================================================================================================================
# Below are the R code to query the DataScienceCoursework SQLite database to answer question three through question seven in the course work guide 
#===========================================================================================================================================================================================================================
# Question 3: R code to find the average price of houses in two years 
query_3 <- dbSendQuery(con, "SELECT DistrictTable.LAN,HOUSEPRICE.LAC,HOUSEPRICE.Wardcode,HOUSEPRICE.Wardname,
(HOUSEPRICE.Mar_2018+HOUSEPRICE.Jun_2018+HOUSEPRICE.Sep_2018+HOUSEPRICE.Dec_2018)/4 AS AVERAGE_2018,
(HOUSEPRICE.Mar_2019+HOUSEPRICE.Jun_2019+HOUSEPRICE.Sep_2019+HOUSEPRICE.Dec_2019)/4 AS AVERAGE_2019,
(HOUSEPRICE.Mar_2018+HOUSEPRICE.Jun_2018+HOUSEPRICE.Sep_2018+HOUSEPRICE.Dec_2018+HOUSEPRICE.Mar_2019+HOUSEPRICE.Jun_2019+HOUSEPRICE.Sep_2019+HOUSEPRICE.Dec_2019)/8 AS AVERAGE_2018_2019
FROM HOUSEPRICE INNER JOIN DistrictTable ON DistrictTable.LAC=HOUSEPRICE.LAC WHERE HOUSEPRICE.Wardcode = 'E05010921'")
dbFetch(query_3)
#============================================================================================================================================================================================================================
# Question 4: R code to find the average increase in house prices between two years
query_4 <- dbSendQuery(con, "SELECT DistrictTable.LAN, HOUSEPRICE.LAC,HOUSEPRICE.Wardcode,HOUSEPRICE.Wardname, 
(((HOUSEPRICE.Mar_2019+HOUSEPRICE.Jun_2019+HOUSEPRICE.Sep_2019+HOUSEPRICE.Dec_2019)/4) - ((HOUSEPRICE.Mar_2018+HOUSEPRICE.Jun_2018+HOUSEPRICE.Sep_2018+HOUSEPRICE.Dec_2018)/4))/((HOUSEPRICE.Mar_2018+HOUSEPRICE.Jun_2018+HOUSEPRICE.Sep_2018+HOUSEPRICE.Dec_2018)/4)*100 
                       AS AVERAGE_INCREASE_2018_2019 FROM HOUSEPRICE INNER JOIN DistrictTable ON DistrictTable.LAC=HOUSEPRICE.LAC WHERE HOUSEPRICE.Wardcode = 'E05010921'")
dbFetch(query_4)
#==============================================================================================================================================================================================================================
# Question 5: R code to find the ward with the highest house price in fourth quater of 2019 (Dec_2019)
query_5 <- dbSendQuery(con, "SELECT DistrictTable.LAN,HOUSEPRICE.LAC,HOUSEPRICE.Wardcode,HOUSEPRICE.Wardname,MAX(HOUSEPRICE.Dec_2019) AS  MAX_DEC_2019
                       FROM HOUSEPRICE INNER JOIN DistrictTable ON DistrictTable.LAC=HOUSEPRICE.LAC")
dbFetch(query_5)
#================================================================================================================================================================================================================================
# Question 6: R code to find the broad bandspeed for a ward with wardcode E05010921
query_6 <- dbSendQuery(con, "SELECT DistrictTable.LAN, BroadBand.Constituency_Code,BroadBand.MSOA11CD,BroadBand.MSOA_name,BroadBand.Average_download_speed_Mbps,BroadBand.Wardcode,HOUSEPRICE.Wardname FROM BroadBand 
                       INNER JOIN HOUSEPRICE ON HOUSEPRICE.Wardcode=BroadBand.Wardcode
                       INNER JOIN DistrictTable ON DistrictTable.LAC=HOUSEPRICE.LAC WHERE HOUSEPRICE.Wardcode = 'E05010921'")
dbFetch(query_6)
#==================================================================================================================================================================================================================================
# Question 7: R code to find the AVERAGE Superfast_availability for a particular ward
query_7<- dbSendQuery(con, "SELECT BroadBand.Wardcode,HOUSEPRICE.Wardname, AVG(BroadBand.Superfast_availability) AS AVERAGE_Superfast_availability FROM BroadBand
                      INNER JOIN HOUSEPRICE ON BroadBand.Wardcode=HOUSEPRICE.Wardcode GROUP BY BroadBand.Wardcode")

dbFetch(query_7)
#===================================================================================================================================================================================================================================
# Question 8: R code to calculate the average council tax charge (BandA, BandB and BandC) for Langford town
query_8<-dbSendQuery(con, "SELECT DistrictTable.LAN,CouncilTaxData.TOWNS_PARISHES,TownsWard.Wardname,TownsWard.Wardcode,
(CouncilTaxData.BandA+CouncilTaxData.BandB+CouncilTaxData.BandC)/3 AS AVERAGE_COUNCIL_TAX FROM CouncilTaxData
INNER JOIN TownsWard ON TownsWard.TOWNS_PARISHES=CouncilTaxData.TOWNS_PARISHES 
INNER JOIN HOUSEPRICE ON HOUSEPRICE.Wardcode=TownsWard.Wardcode
INNER JOIN DistrictTable ON DistrictTable.LAC=HOUSEPRICE.LAC WHERE CouncilTaxData.TOWNS_PARISHES = 'Langford'")
dbFetch(query_8)
#=====================================================================================================================================================================================================================================
# Question 9:R code to find the difference in Band A council tax for Alvescot and Bampton of West Oxfordshire district
query_9 <- dbSendQuery(con, "SELECT T1.TOWNS_PARISHES,T1.BandA, T2.TOWNS_PARISHES,T2.BandA, T2.BandA-T1.BandA AS DIFFRENCE_BTW_Alvescot_Bampton FROM 
(SELECT BandA, TOWNS_PARISHES FROM CouncilTaxData WHERE TOWNS_PARISHES = 'Alvescot') AS T1, 
                       (SELECT BandA, TOWNS_PARISHES FROM CouncilTaxData WHERE TOWNS_PARISHES = 'Bampton') AS T2")
dbFetch(query_9)
#======================================================================================================================================================================================================================

