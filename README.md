# Bikes_Data_Warehouse_ETL
Python Script to first extract the data from CSV files and then perform the transformations (ET)  and SCD and finally load the data in a data warehouse of SQL server.
Creation of  a small Datawarehouse in Microsoft SQL server which is functionally and technically able to answer the Business Questions related to the Dataset. Assumption is source files will come from the OLTP system time to time and then we will have to process them and load them into an Operational Data Store. From Operational Datastore I have created a DataMart/Data Warehouse which connects with the Reporting tool MicroStrategy which answers the Business question on this Data. ![image](https://user-images.githubusercontent.com/83393290/186273417-e29f0a6f-cb4b-4b1f-be46-c32ac4075abb.png)

The OLTP source Data is in the form of CSV files and these files are to be extracted and transformed before loading them to the Operational Datastore. 
I had used the below attached Python Script to first extract the data from CSV files and then perform the transformations as follows:
1)	For Customer CSV file I found that the data was coming with special characters on the first_name and Last_name column so I have transfomed the data to make sure the special characters are removed from those columns.
2)	Also, in the customer CSV I found that the data was duplicated in the file so I removed all the duplicates from the file before loading and did this transformation across all my Source CSV files.
3)	Also, in the customer file there were few fields which were not of any use to my data analytics like wealth_segment or Deceased_indicator, so I removed these columns before I loaded the data into ODS.
4)	As a matter of Fact, I was more interested to know the Age Range in which the customer lies and so given his/her Date of birth (DOB) I calculated the Age for each incoming dataset of customer as part of transformation and added the Agerange column in the source dataset of customer, that will then be loaded to ODS.
After this part of Transformation is done for each CSV files, I have loaded the CSV files in the Bikes Database of my SQL SERVER through python script itself. I have loaded each file in their corresponding database tables based on the Slowly Changing Dimension 

Once the data reaches in ODS( Bikes Database in SQL server) the Business/Technical Team can access this data and create their own DataMart/ Datawarehouse(Bikes_DWH database in SQL server). In this case I have created a DATA Warehouse which answers questions on Sales Related Information for the Bikes. Below is the ERD for the same
![image](https://user-images.githubusercontent.com/83393290/186273698-8772893b-5771-40cc-b972-0ae289130291.png)
