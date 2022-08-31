----Header---------------------------------------------------------------------------------------------------------------------------------------
/*
This Process creates operational data warehouse for Bikes Database
Refresh Freq= Daily - End of Business Day
Create Date=2021-12-03
Modify Date=2021-12-05
KPIs= order count, product count, Rating, Sale Amount
*/
----Adhoc Analysis---------------------------------------------------------------------------------------------------------------------------------------
/*
SELECT * from [dbo].[Customer];
SELECT * from [dbo].SalesOrderitems;
SELECT * from [dbo].[SalesOrder];
SELECT * from [dbo].ProductDetail
SELECT * from [dbo].[DATETAB];
SELECT * from [dbo].[Address];
SELECT * from [dbo].[Product] order by PRODUCTID;
*/
----Pre Steps---------------------------------------------------------------------------------------------------------------------------------------
USE [Bikes];
----Product Fact Step---------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM Bikes_DWH.dbo.Prdct_Sm_Fct;
INSERT INTO Bikes_DWH.dbo.Prdct_Sm_Fct 
SELECT
getdate()				as DWH_CYC_DT
,OI.[PRODUCTID]			as Prdct_ID
,O.[Date]				as Act_Perd_Dt
,sum(OI.[GROSSAMOUNT])	as Sale_Amt
,sum(OI.[QUANTITY])		as Prdct_Cnt
--INTO Bikes_DWH.dbo.Prdct_Sm_Fct 
FROM 
[dbo].[SalesOrderItems] OI
join
[dbo].[SalesOrder] O
ON 
o.[SalesOrderID]= OI.[SalesOrderID]
GROUP BY
OI.[PRODUCTID]			
,O.[Date]			 ;
----Order Summary Fact Step---------------------------------------------------------------------------------------------------------------------------------------
INSERT INTO Bikes_DWH.dbo.Ordr_Sm_Fct 
SELECT
getdate()				as DWH_CYC_DT
,O.[SalesOrderID]		as Ordr_ID
,O.[customer_id]		as Cust_ID
,O.[StoreID]			as Str_ID
,O.[Date]				as Act_Perd_Dt
,Sum(1)																	as Ordr_Cnt
,sum(CASE WHEN [Ordertype] =  'Online' THEN 1 ELSE 0 END)				as On_Ordr_Cnt
,sum(CASE WHEN [Ordertype] <> 'Online' THEN 1 ELSE 0 END)				as Off_Ordr_Cnt
,Sum([GROSSAMOUNT])														as Ordr_Amt
,sum(CASE WHEN [Ordertype] =  'Online' THEN [GROSSAMOUNT] ELSE 0 END)	as On_Ordr_Amt
,sum(CASE WHEN [Ordertype] <> 'Online' THEN [GROSSAMOUNT] ELSE 0 END)	as Off_Ordr_Amt
,Avg([RATING])															as Rtng_Val
,sum(CASE WHEN [Ordertype] <> 'Online' THEN [RATING] ELSE 0 END)		as Off_Rtng_Val
,sum(CASE WHEN [Ordertype] =  'Online' THEN [RATING] ELSE 0 END)		as On_Rtng_Val
--INTO Bikes_DWH.dbo.Ordr_Sm_Fct 
FROM 
[dbo].[SalesOrder] O
/*---CDC-------------*/
Left JOIN 
Bikes_DWH.dbo.Ordr_Sm_Fct  Fct
ON 
o.[SalesOrderID]= FCT.Ordr_ID
where
FCT.Ordr_ID is null
/*---------------*/
GROUP BY
 O.[SalesOrderID]		
,O.[customer_id]		
,O.[StoreID]			
,O.[Date]	;					 ;

----Order Detail Fact Step---------------------------------------------------------------------------------------------------------------------------------------
INSERT INTO Bikes_DWH.dbo.Ordr_Dtl_Fct 
SELECT
getdate()				as DWH_CYC_DT
,OI.[PRODUCTID]			as Prdct_ID
,O.[SalesOrderID]		as Ordr_ID
,O.[customer_id]		as Cust_ID
,O.[StoreID]			as Str_ID
,O.[Date]				as Act_Perd_Dt
,sum(OI.[GROSSAMOUNT])	as Sale_Amt
,sum(OI.[QUANTITY])		as Prdct_Cnt
--INTO Bikes_DWH.dbo.Ordr_Dtl_Fct 
FROM 
[dbo].[SalesOrderItems] OI
join
[dbo].[SalesOrder] O
ON 
o.[SalesOrderID]= OI.[SalesOrderID]
/*---CDC-------------*/
Left JOIN 
Bikes_DWH.dbo.Ordr_Dtl_Fct  Fct
ON 
o.[SalesOrderID]= FCT.Ordr_ID
and oi.[PRODUCTID]= FCT.Prdct_ID
where
FCT.Ordr_ID is null
/*---------------*/
GROUP BY
 OI.[PRODUCTID]			
,O.[SalesOrderID]		
,O.[customer_id]		
,O.[StoreID]			
,O.[Date]		;
----Customer Dimension---------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM  Bikes_DWH.dbo.Cust_Dim;
INSERT INTO  Bikes_DWH.dbo.Cust_Dim
SELECT
getdate()			as DWH_CYC_DT
,[customer_id]		as  Cust_ID
,[first_name]		as 	Cust_F_Nm
,[last_name]		as 	Cust_L_Nm
,[gender]			as 	Cust_Gndr
,[DOB]				as 	Cust_Brth_Dt
,[Age]				as 	Cust_Age
,[Agerange]			as 	Cust_Age_Grp
--INTO Bikes_DWH.dbo.Cust_Dim 
 FROM 
 [dbo].[Customer];
 ----Store Dimension---------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM  Bikes_DWH.dbo.Str_Dim;
INSERT INTO  Bikes_DWH.dbo.Str_Dim
SELECT
getdate()			as DWH_CYC_DT
,S.[StoreID]			as Str_ID
,S.[phone]				as Str_Phn_Nbr
,S.[manager]			as Str_Mngr_Nm
,a.[CITY]				as Str_City
,a.[COUNTRY]			as Str_Cntry
,a.[REGION]				as Str_Rgn
,a.[POSTALCODE]			as Str_Zip
--INTO Bikes_DWH.dbo.Str_Dim
FROM 
[dbo].[Store] S
left join
[dbo].[Address] a
on S.[ADDRESSID] = a.[ADDRESSID];
 ----Product Dimension---------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM  Bikes_DWH.dbo.Prdct_Dim;
INSERT INTO  Bikes_DWH.dbo.Prdct_Dim
SELECT
getdate()					as DWH_CYC_DT
,P.[PRODUCTID]				as Prdct_ID
,PD.[PRODUCT_NAME]			as Prdct_Nm
,P.[PRICE]					as Prdct_Cst
,PC.[PRODCATEGORYNAME]		as Prdct_Ctg_Nm
,BP.[COMPANYNAME]			as Ptrn_Nm
,BP.[EMAILADDRESS]			as Ptnr_Email
,a.[CITY]					as Ptnr_City
,a.[COUNTRY]				as Ptnr_Cntry
,a.[REGION]					as Ptnr_Rgn
,a.[POSTALCODE]				as Ptnr_Zip
--INTO Bikes_DWH.dbo.Prdct_Dim
 FROM 
[dbo].[Product] P
left JOIN
[dbo].[ProductCategory] PC
ON P.[PRODCATEGORYID] = PC.[PRODCATEGORYID]
left JOIN
[dbo].[ProductDetail] PD
ON P.[PRODUCTID] = PD.[PRODUCTID]
left JOIN
[dbo].[BusinessPartner]  BP
ON P.[PARTNERID] = BP.[PARTNERID]
left join
[dbo].[Address] a
on BP.[ADDRESSID] = a.[ADDRESSID]
where P.current_flag = 1; 
----Date Dimension---------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM  Bikes_DWH.dbo.Act_Perd_Dim;
INSERT INTO  Bikes_DWH.dbo.Act_Perd_Dim
SELECT
getdate()								as DWH_CYC_DT
,[Date]									as Act_Perd_Dt
,[Date_Day]								as Act_Perd_Day
,[Date_WEEKDAYID]						as Act_Perd_Wk_Day_Nbr
,[Date_WEEKDAY]							as Act_Perd_Wk_Day
,[Date_week]							as Act_Perd_Wk
,[DATE_Quarter]							as Act_Perd_Qtr_Nbr
, CASE 
WHEN [DATE_Quarter] = 1 then 'Q1'
WHEN [DATE_Quarter] = 2 then 'Q2'
WHEN [DATE_Quarter] = 3 then 'Q3'
WHEN [DATE_Quarter] = 4 then 'Q4'
end										as Act_Perd_Qtr_Nm
,[DATE_YEAR]							as Act_Perd_Yr
,[DATE_YEAR]*100 + [DATE_MONTH]			as Act_Perd_Yr_Mo_Nbr
,[DATE_MONTH]							as Act_Perd_Mo_Nbr
,[DATE_MONTHNAME]						as Act_Perd_Mo
,CASE WHEN [DATE_YEAR]=year(getdate())			 then 'Y'ELSE 'N' END			as Act_Perd_CYTD
,CASE WHEN [DATE_YEAR]=year(getdate())-1		 then 'Y'ELSE 'N' END		as Act_Perd_PYTD
,CASE WHEN [Date] > DATEADD(DAY, 1-DATEPART(WEEKDAY, DATEADD(DAY,0,cast(getdate() as date))), DATEADD(DAY,0,cast(getdate() as date)))   then 'Y'ELSE 'N' END		        as Act_Perd_CW
,CASE WHEN [Date] > DATEADD(DAY, 1-DATEPART(WEEKDAY, DATEADD(DAY,-7,cast(getdate() as date))), DATEADD(DAY,-7,cast(getdate() as date)))   then 'Y'ELSE 'N' END		        as Act_Perd_PW
,CASE WHEN [Date] > DATEADD(DAY, 1-DATEPART(WEEKDAY, DATEADD(DAY,-7*4,cast(getdate() as date))), DATEADD(DAY,-7*4,cast(getdate() as date)))   then 'Y'ELSE 'N' END		    as Act_Perd_4W
,CASE WHEN [Date] > DATEADD(DAY, 1-DATEPART(WEEKDAY, DATEADD(DAY,-7*13,cast(getdate() as date))), DATEADD(DAY,-7*13,cast(getdate() as date)))   then 'Y'ELSE 'N' END		as Act_Perd_13W
--INTO Bikes_DWH.dbo.Act_Perd_Dim
 FROM 
[dbo].[DATETAB]

 commit;
 



 


 













