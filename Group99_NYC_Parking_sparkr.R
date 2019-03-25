# loading dplyr
library(dplyr)
# Load SparkR
spark_path <- '/usr/local/spark'
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = spark_path)
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

# Initialise the sparkR session
sparkR.session(master = "yarn-client", sparkConfig = list(spark.driver.memory = "1g"))


#Reading Dataframe:

parking_data <- read.df("hdfs:///common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", source = "csv", 
                        inferSchema = "true", header = "true")
#Examine data:
head(parking_data)
nrow(parking_data)    ## 10803028 Rows
ncol(parking_data)    ## 10 Coulmns
str(parking_data)
#There are many rows which are not for year 2017.We are expected to Analyze only for year 2017.

# Before executing any hive-sql query from RStudio, you need to add a jar file in RStudio 
sql("ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar")


createOrReplaceTempView(parking_data,"par_view")

#checking Data for years included:

yyea <- SparkR::sql("select year(`Issue Date`) as Year,count(*) from par_view group by Year")
collect(yyea)
#Year 2017 has only 5431918 number of rows.Hence,only considering for Year 2017

parking_data_2017 <- SparkR::sql("select * from par_view where year(`Issue Date`)= 2017 ")
createOrReplaceTempView(parking_data_2017,"par_view_2017")
head(SparkR::sql("select count(*) from par_view_2017"))

#Hence number of rows which have year only 2017 are: 5431918

str(parking_data_2017)
nrow(parking_data_2017)

# Finding total tickets for the year 2017:

#Checking count of unique rows
createOrReplaceTempView(parking_data_2017,"park_view_2017")
park_nyc_totals <- SparkR::sql("SELECT count(distinct(`Summons Number`)) from park_view_2017")
head(park_nyc_totals)
count(unique(parking_data_2017))
#Unique rows are 5431918 
#Total Tickets count for the year 2017 is :5431918

# Before executing any hive-sql query from RStudio, you need to add a jar file in RStudio 
#sql("ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar")

createOrReplaceTempView(parking_data_2017, "parking_data_view")


#Find out the number of unique states from where the cars that got parking tickets came from. 

#First Checking Total number of Unique Registration State
collect(select(parking_data_2017, countDistinct(parking_data_2017$`Registration State`)))
#65 unique Registration State


result <- summarize(groupBy(parking_data_2017, parking_data_2017$`Registration State`),
                    count = n(parking_data_2017$`Registration State`))
head(arrange(result,desc(result$count)),65)

#There is a numeric entry '99' in the column which should be corrected.Replacing with the state having 
# maximum entries,which is NY.


parking_data_2017$`Registration State` <- ifelse(parking_data_2017$`Registration State`=="99","NY",parking_data_2017$`Registration State`)

#Checking unique states again:

result <- summarize(groupBy(parking_data_2017, parking_data_2017$`Registration State`),
                    count = n(parking_data_2017$`Registration State`))
head(arrange(result,desc(result$count)),65)



###Aggregation tasks

#1.How often does each violation code occur? Display the frequency of the top five violation codes.

#Using SQL:
createOrReplaceTempView(parking_data_2017, "parking_data_view")
gt <- SparkR::sql("select `Violation Code` as Violation_code , count(*) as count from parking_data_view group by Violation_code order by count desc limit 5")
head(gt)

collect(select(parking_data_2017, countDistinct(parking_data_2017$`Violation Code`)))
#There are 100 unique Violation code

#Using dplyr
Violation_code_agg <- summarize(group_by(parking_data_2017,parking_data_2017$`Violation Code`)
                                ,count = n(parking_data_2017$`Violation Code`))
head(arrange(Violation_code_agg,desc(Violation_code_agg$count)),5)
#Top 5 Violation code with their counts/Frequency are as follows:
#   Violation Code   count                                                        
#1             21  768087
#2             36  662765
#3             38  542079
#4             14  476664
#5             20  319646

top_five_vcs <- data.frame(head(arrange(Violation_code_agg,desc(Violation_code_agg$count)),5))
#Vizualizing Frequency of each Violation code:
plot <- ggplot(top_five_vcs, aes(x = Violation.Code, y = count)) +
  geom_bar(stat = "identity") +
  xlab("Violation_Code") + ylab("Frequency")
plot

#2.How often does each 'vehicle body type' get a parking ticket? How about the 'vehicle make'? 
#For Vehical body:
Vehical_body_agg <- summarize(group_by(parking_data_2017,parking_data_2017$`Vehicle Body Type`)
                              ,count = n(parking_data_2017$`Vehicle Body Type`))
head(arrange(Vehical_body_agg,desc(Vehical_body_agg$count)),5)

#Hence,Count of 'vehicle body type' getting a parking ticket are:

#       Vehicle Body Type   count                                                     
#1              SUBN      1883954
#2              4DSD      1547312
#3               VAN      724029
#4              DELV      358984
#5               SDN      194197

#For Vehical make:
Vehical_make_agg <- summarize(group_by(parking_data_2017,parking_data_2017$`Vehicle Make`)
                              ,count = n(parking_data_2017$`Vehicle Make`))
head(arrange(Vehical_make_agg,desc(Vehical_make_agg$count)),5)

#Hence,Count of 'vehicle Make' getting a parking ticket are:

#    Vehicle Make   count                                                          
#1         FORD    636844
#2        TOYOT    605291
#3        HONDA    538884
#4        NISSA    462017
#5        CHEVR    356032


#3.A precinct is a police station that has a certain zone of the city under its command. 
#Find the (5 highest) frequency of tickets for each of the following:
#'Violation Precinct' (this is the precinct of the zone where the violation occurred).
#' Using this, can you make any insights for parking violations in any specific areas of the city?



#Unique 'Violation Precinct'

collect(select(parking_data_2017, countDistinct(parking_data_2017$`Violation Precinct`)))  
#171 unique 'Violation Precinct'

Violent_Precinct <- summarize(group_by(parking_data_2017,parking_data_2017$`Violation Precinct`)
                              ,count = n(parking_data_2017$`Violation Precinct`))
head(arrange(Violent_Precinct,desc(Violent_Precinct$count)),6)  

#Since '0' Precinct is are the erroneous entries.So Top 5 'Violation Precinct' are:
#    Violation Precinct   count                                                    
#1                 19  274445
#2                 14  203553
#3                  1  174702
#4                 18  169131
#5                114  147444

#for Issuer Precinct

Issuer_Precinct <- summarize(group_by(parking_data_2017,parking_data_2017$`Issuer Precinct`)
                             ,count = n(parking_data_2017$`Issuer Precinct`))
head(arrange(Issuer_Precinct,desc(Issuer_Precinct$count)),6)

#Top 5 'Issuer Precinct' with their counts are:
#    Issuer Precinct   count                                                       
#1              19  266961
#2              14  200495
#3               1  168740
#4              18  162994
#5             114  144054


##4.Find the violation code frequency across three precincts which have issued the most number of tickets - do these precinct zones have an exceptionally high frequency of certain violation codes? 
#Are these codes common across precincts? 

#Top 3 Issuer Percinct are:19,14 and 1

createOrReplaceTempView(parking_data_2017, "parking_data_tbl")

xc <- SparkR::sql("select `Issuer Precinct`,`Violation Code`,count(`Violation Code`) as Counts \
                  from parking_data_tbl where `Issuer Precinct` in (19,14,1) \
                  group by`Issuer Precinct`, `Violation Code` order by Counts desc")

head(xc,3)
#Hence,Violation code frequency across three precincts which have issued the most number of tickets are:
#   Issuer Precinct Violation Code Counts                                        
#1               19             46  48445
#2               14             14  45036
#3                1             14  38354


#There isn't any violation code which is exceptionally high and code '14' is common across Precints 14 & 1.

#########5.find out the properties of parking violations across different times of the day:

#Exploring NA's and Null:
colnames(parking_data_2017)

park_nyc_nulls <- SparkR::sql("select COUNT(*) row_counts,
                              sum(case when 'Plate ID' IS NULL
                              then 1
                              else 0
                              end) plate_id_null,
                              sum(case when 'Registration State' IS NULL
                              then 1
                              else 0
                              end) reg_state_null,
                              sum(case when 'Issue Date' IS NULL
                              then 1
                              else 0
                              end) issue_date_null,
                              sum(case when 'Violation Code' IS NULL
                              then 1
                              else 0
                              end) vio_code_null,
                              sum(case when 'Vehicle Body Type' IS NULL
                              then 1
                              else 0
                              end) vb_type_null,
                              sum(case when 'Vehicle Make' IS NULL
                              then 1
                              else 0
                              end) veh_mak_null,
                              sum(case when 'Violation Precinct' IS NULL
                              then 1
                              else 0
                              end) vio_prec_null,
                              sum(case when 'Issuer Precinct' IS NULL
                              then 1
                              else 0
                              end) issu_prec_null,
                              sum(case when 'Violation Time' IS NULL
                              then 1
                              else 0
                              end) vio_time_null
                              from parking_data_tbl")
head(park_nyc_nulls)

# NO Nulls in data

#5(b) The Violation Time field is specified in a strange format. 
# Find a way to make this into a time attribute that you can use to divide into groups.

# 1st 2 Numbers in the "Violation Time" coulmn states the "HOUR".So extracting 1st two numbers and marking them as 'hour'                          
parking_data_2017 <- parking_data_2017 %>% withColumn("hour",substr(parking_data_2017$`Violation Time`,1,2))
head(parking_data_2017)

parking_data_2017 <- parking_data_2017 %>% withColumn("minute",substr(parking_data_2017$`Violation Time`,3,4))
parking_data_2017 <- parking_data_2017 %>% withColumn("A/P",substr(parking_data_2017$`Violation Time`,5,5))

head(parking_data_2017)
#Now converting Violation Time into timestamp and extracting hour in different coulmn named as"hour"

parking_data_2017$`Violation Time_Modified` <- concat_ws(sep=":", parking_data_2017$hour, parking_data_2017$minute)
parking_data_2017$`Violation Time_Modified` <- to_timestamp(parking_data_2017$`Violation Time_Modified`,"HH:mm")
parking_data_2017$hour <- hour(parking_data_2017$`Violation Time_Modified`)
#Extracting hour in 24hr format .Hence,All the coulmns which have time corresponding to "P" needs to be added to 12

parking_data_2017$hour <- ifelse(parking_data_2017$`A/P` == "P",parking_data_2017$hour+12,parking_data_2017$hour)
head(parking_data_2017)

#5 (C) Divide 24 hours into six equal discrete bins of time. The intervals you choose are at your discretion. 
#For each of these groups, find the three most commonly occurring violations.

#Divided Violation Time hours into six equal discrete bins of time which are:
#1_hour ,2_hour,3_hour,4_hour,5_hour,6_hour

parking_data_2017 <- parking_data_2017 %>% withColumn("time",
                                                      ifelse(parking_data_2017$hour>=0 & parking_data_2017$hour<4,'1_hour',
                                                             ifelse(parking_data_2017$hour>=4 & parking_data_2017$hour < 8 ,'2_hour',
                                                                    ifelse(parking_data_2017$hour>=8 & parking_data_2017$hour<12,'3_hour',
                                                                           ifelse(parking_data_2017$hour>=12 & parking_data_2017$hour<16,'4_hour',
                                                                                  ifelse(parking_data_2017$hour >=16 & parking_data_2017$hour <20,'5hour','6_hour'))))))

head(parking_data_2017)

createOrReplaceTempView(parking_data_2017 ,"park_count")

# Finding for each of these groups three most commonly occurring violations.
gtyy <- SparkR::sql("select time, `Violation Code` as Violation_code , count(*) as count from park_count group by time, Violation_code")
head(gtyy)
createOrReplaceTempView(gtyy, "gtyy_view")

rank <- SparkR::sql("select *, dense_rank() over(partition by time order by count desc) as ra from gtyy_view")
head(rank,30)
createOrReplaceTempView(rank, "rank_view")

head(SparkR::sql("select time,Violation_code,count from rank_view where ra <= 3"),18)
#Hence,the three most commonly occurring violations for each group is:

#   time   Violation_code   count                                                 
#1   5hour             38   102855
#2   5hour             14   75902
#3   5hour             37   70345

#4  6_hour             36   101991
#5  6_hour             38   76314
#6  6_hour             21   72572

#7  1_hour             21   34703
#8  1_hour             40   23628
#9  1_hour             14   14168

#10 3_hour             21   598068
#11 3_hour             36   348165
#12 3_hour             38   176570

#13 4_hour             38   184829
#14 4_hour             36   184293
#15 4_hour             37   130692

#16 2_hour             14   74113
#17 2_hour             40   60652
#18 2_hour             21   57896


#5(D).For the three most commonly occurring violation codes, 
#         find the most common time of the day (in terms of the bins from the previous part)?
createOrReplaceTempView(parking_data_2017 ,"park_view")

#vio_code <- 
head(SparkR::sql("select `Violation Code` , count(*) as count from park_view group by `Violation Code` order by count desc"),3)


#Using dplyr
Violation_ <- summarize(group_by(parking_data_2017,parking_data_2017$`Violation Code`)
                        ,count = n(parking_data_2017$`Violation Code`))
head(arrange(Violation_,desc(Violation_$count)),3)
#Top 3 most commonly occurring Violation code are : 21,36 and 38
#   Violation Code  count                                                         
#1             21 768087
#2             36 662765
#3             38 542079

vio_top3 <- SparkR::sql("select * from park_view where `Violation Code` in (21,36,38)")
head(vio_top3)
createOrReplaceTempView(vio_top3 ,"vio_top3_view")

head(SparkR::sql("select time,count(*) as count from vio_top3_view group by time order by count desc"),3)

#Most common time of the day for Top 3 Violation code are :  

#    time   count                                                                
#1. 3_hour 1122803
#2. 4_hour  373711
#3. 6_hour  250877


#### 6.Let's try and find some seasonality in this data
## 6(A), First, divide the year into some number of seasons, and find frequencies of tickets for each season

parking_data_2017$`Issue Date` <- to_date(parking_data_2017$`Issue Date`)
parking_data_2017 <- parking_data_2017 %>% withColumn("Month_Issue_date",month(parking_data_2017$`Issue Date`))
# Season Binning:
# Spring: 1 March to 31st May
# Summer: 1 June to 31 August
# Autumn: 1st September to 30 November
# Winter: 1st December to 28th February

parking_data_2017 <- parking_data_2017 %>% withColumn("season",ifelse(parking_data_2017$Month_Issue_date >=3 & parking_data_2017$Month_Issue_date<=5,'Spring',
                                                                      ifelse(parking_data_2017$Month_Issue_date>=6 & parking_data_2017$Month_Issue_date <=8 ,'Summer',
                                                                             ifelse(parking_data_2017$Month_Issue_date>=9 & parking_data_2017$Month_Issue_date<=12,'Autumn','Winter'))))

overall_counts <- summarize(groupBy(parking_data_2017, parking_data_2017$season),
                            count = n(parking_data_2017$season))
head(arrange(overall_counts, desc(overall_counts$count)))

#Frequencies for each Season is as follows:
#  season   count                                                                
#1 Spring  2873383
#2 Winter  1704332
#3 Summer  852866
#4 Autumn    1337

## 6 (B) Then, find the three most common violations for each of these seasons.

createOrReplaceTempView(parking_data_2017, "par_view")


seas <- SparkR::sql("select season, `Violation Code` as Violation_code , count(*) as count from par_view group by season, Violation_code")
head(seas)
createOrReplaceTempView(seas, "seas_view")

rank_season <- SparkR::sql("select *, dense_rank() over(partition by season order by count desc) as ra from seas_view")
head(rank_season,200)

createOrReplaceTempView(rank_season, "rank_season_view")


head(SparkR::sql("select season,Violation_code,count from rank_season_view where ra <= 3"),12)

#The three most common violations for each of these seasons.

# season     Violation_code    count                                                 
#1  Spring             21   402424
#2  Spring             36   344834
#3  Spring             38   271167

#4  Summer             21   127352
#5  Summer             36    96663
#6  Summer             38    83518

#7  Autumn             46    308
#8  Autumn             21    172
#9  Autumn             40    160

#10 Winter             21   238139
#11 Winter             36   221268
#12 Winter             38   187383


### 7. The fines collected from all the parking violation constitute a revenue source for the NYC police department. 
###Let's take an example of estimating that for the three most commonly occurring codes.

#7.(A) Find total occurrences of the three most common violation codes

vio_counts <- summarize(groupBy(parking_data_2017, parking_data_2017$`Violation Code`),
                        count = n(parking_data_2017$`Violation Code`))
head(arrange(vio_counts, desc(vio_counts$count)),3)

#Three most common Violation codes and their occurences are:
#    Violation Code   count                                                         
#1             21    768087
#2             36    662765
#3             38    542079

# 7. (B) Then, visit the website:http://www1.nyc.gov/site/finance/vehicles/services-violation-codes.page
#It lists the fines associated with different violation codes. They're divided into two categories, one for the highest-density locations of the city, the other for the rest of the city.
#For simplicity, take an average of the two.

#Violation Code      Average
#   21               55
#   36               50
#   38               50

Violation_Code <- c(21,36,38)
Average <- c(55,50,50)

Average_violation <- data.frame(Violation_Code,Average)
class(Average_violation)

#Converting it into SparkR data frame
Average_violation <- as.DataFrame(Average_violation)
class(Average_violation)

#  7.  (C)  Using this information, find the total amount collected for the three violation codes with maximum tickets. 
#          State the code which has the highest total collection.

# Three Violation codes with maximum tickets are: 21,36,38
#Taking dataframe with only Violation codes with maximum tickets
createOrReplaceTempView(parking_data_2017,"data_2017")
v_top3 <- SparkR::sql("select * from data_2017 where `Violation Code` in (21,36,38)")
head(v_top3)

createOrReplaceTempView(v_top3,"top_vio_code_view")

v_top3_count <- SparkR::sql("select `Violation Code` ,count(*) as count from top_vio_code_view group by `Violation Code` order by count desc ")
head(v_top3_count)


createOrReplaceTempView(v_top3_count,"v_top3_count_view")
createOrReplaceTempView(Average_violation,"Average_violation_view")

# Merging Average Amount Coulmn too using inner join:
aver_vio <- SparkR::sql("select * from v_top3_count_view inner join Average_violation_view on v_top3_count_view.`Violation Code`=Average_violation_view.Violation_Code")
head(aver_vio)
#  Violation Code    count     Average                                  
#1             36  662765         50
#2             21  768087         55
#3             38  542079         50

#Amount Collected can be calculated by muntiplying 'Average' amount with the 'count'
c <- aver_vio
head(c)

aver_vio$Amount_collected <- aver_vio$count * aver_vio$Average
head(aver_vio)

#Hence,we have Amount Collected for each Violation Code:
# Violation Code  count      Average   Amount_collected                 
#1         36    662765         50         33138250
#2         21    768087         55         42244785
#3         38    542079         50         27103950


createOrReplaceTempView(aver_vio,"aver_vio_View")
head(SparkR::sql("select * from aver_vio_View order by Amount_collected desc "))

#The Violation code: 21 has the highest total collection which is $42244785.

#7(D). What can you intuitively infer from these findings?

#We can say that Violation code 21 is something that is occuring more often and is fetching maximum of the fine collection Amount.

