CREATE DATABASE Bikes
go
use Bikes;

--(Date , Date_Day, Date_WEEKDAY, Date_week , Date_WEEKDAYID,DATE_MONTH,DATE_MONTHNAME,DATE_Quarter, DATE_YEAR)
(select distinct
 CONVERT(date, d) Date ,
 DATEPART(DAY,       d) Date_Day,
 DATENAME(WEEKDAY,   d) Date_WEEKDAY, 
 DATEPART(WEEK,      d) Date_week,
 DATEPART(WEEKDAY,   d) Date_WEEKDAYID,
 DATEPART(MONTH,     d) DATE_MONTH,
 DATENAME(MONTH,     d)DATE_MONTHNAME,
 DATEPART(Quarter,   d) DATE_Quarter,
 DATEPART(YEAR,      d) DATE_YEAR
into [Bikes].dbo.DATETAB
from (
		select
		 DATEADD(year, 1,cast(getdate() - 366*5  + ROW_NUMBER()over (order by  getdate() ) as date) ) as d
		from
		[NetFlix].dbo.Rental_history
) T
where 
d
between 
getdate() - 366*5 
and 
getdate()  + 365);
commit;

ALTER TABLE DATETAB
ALTER COLUMN DATE Date NOT NULL
GO
ALTER TABLE DATETAB
ADD CONSTRAINT PK_DATETAB_Date 
PRIMARY KEY ( Date)
GO

use Bikes;
CREATE TABLE Customer(
customer_id numeric(10)	NOT NULL,
first_name varchar(32)	NOT NULL,
last_name varchar(32) ,
gender varchar(10)	NOT NULL,
DOB date NOT NULL,
Age numeric(5) NOT NULL,
Agerange varchar(20) NOT NULL,
CONSTRAINT Customer_customerid_PK PRIMARY KEY (customer_id));

CREATE TABLE Address(
ADDRESSID numeric(10)	NOT NULL,
CITY varchar(32)	NOT NULL,
COUNTRY varchar(50)	NOT NULL,
REGION  varchar(50)	NOT NULL,
POSTALCODE numeric(10)	NOT NULL,
CONSTRAINT ADDRESS_ADDRESSID_PK PRIMARY KEY (ADDRESSID));



CREATE TABLE BusinessPartner(
PARTNERID numeric(10)	NOT NULL,
EMAILADDRESS  varchar(100)	,
ADDRESSID numeric(10)	NOT NULL,
COMPANYNAME varchar(100),
CONSTRAINT BusinessPartners_PARTNERID_PK PRIMARY KEY (PARTNERID));


CREATE TABLE ProductCategory(
PRODCATEGORYID varchar(10)	NOT NULL,
PRODCATEGORYNAME  varchar(100) NOT NULL,
CONSTRAINT ProductCategory_PRODCATEGORYID_PK PRIMARY KEY (PRODCATEGORYID));

CREATE TABLE Product(
PID numeric Identity(1,1),
PRODUCTID varchar(10)	NOT NULL,
PRODCATEGORYID  varchar(10) NOT NULL,
PARTNERID  numeric(10)	NOT NULL,
PRICE numeric(10) NOT NULL,
CURRENT_FLAG numeric NOT NULL,
effective_timestamp datetime DEFAULT getdate(),
expire_timestamp datetime,
CONSTRAINT Product_PID_PK PRIMARY KEY (PID));
commit;


CREATE TABLE ProductDetail(
PRODUCTID varchar(10)	NOT NULL,
PRODUCT_NAME  varchar(100) NOT NULL,
CONSTRAINT ProductDetail_PRODUCTID_PK PRIMARY KEY (PRODUCTID));


CREATE TABLE Store(
StoreID numeric(10)	NOT NULL,
manager  varchar(32)	,
AddressID  numeric(10)	NOT NULL,
phone  varchar(20)	,
CONSTRAINT Store_StoreID_PK PRIMARY KEY (StoreID));

CREATE TABLE SalesOrder(
SalesOrderID numeric(10)	NOT NULL,
SALESORG  varchar(10)	NOT NULL,
GROSSAMOUNT numeric(10) NOT NULL,
Ordertype varchar(10) NOT NULL,
StoreID numeric(10)	NOT NULL,
Date  date	NOT NULL,
RATING numeric(10) ,
customer_id numeric(10)	NOT NULL,
CONSTRAINT SalesOrder_SALESORDERID_PK PRIMARY KEY (SALESORDERID));



CREATE TABLE SalesOrderItems(
SalesOrderItemsID numeric(10)	NOT NULL,
SalesOrderID numeric(10)	NOT NULL,
PRODUCTID  varchar(10) NOT NULL,
GROSSAMOUNT  numeric(10)	NOT NULL,
QUANTITY numeric(10) NOT NULL,
CONSTRAINT SalesOrderItems_SalesOrderItemsID_PK PRIMARY KEY (SalesOrderItemsID));

commit;

drop table [dbo].[Address]
drop table [dbo].[BusinessPartner]
drop table [dbo].[Customer]
drop table [dbo].[Product]
drop table [dbo].[ProductCategory]
drop table [dbo].[ProductDetail]
drop table [dbo].[SalesOrder]
drop table [dbo].[SalesOrderItems]
drop table [dbo].[Store]
commit;

select * from [dbo].[Address]
select * from [dbo].[BusinessPartner]
select * from [dbo].[Customer]
select * from [dbo].[Product]
select * from [dbo].[ProductCategory]
select * from [dbo].[ProductDetail]
select * from [dbo].[SalesOrder]
select * from [dbo].[SalesOrderItems]
select * from [dbo].[Store]


