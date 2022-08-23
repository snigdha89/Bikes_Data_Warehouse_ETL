import os
import pandas as pd
import sqlalchemy as sqla
import re
from sqlalchemy.orm import sessionmaker
from datetime import datetime, date
pd.options.mode.chained_assignment = None

# create sqlalchemy engine

connection_uri = (
    "mssql+pyodbc://LAPTOP-8FUPBF90/Bikes?driver=ODBC+Driver+17+for+SQL+Server"
)
engine = sqla.create_engine(connection_uri)

Session = sessionmaker(engine)
session = Session()

################## For Customer Table ETL and SCD1 Implementation #####################

Customer_path = os.path.abspath('Customer.csv')
df_Customer = pd.read_csv(Customer_path)


df_Customer = df_Customer[['customer_id', 'first_name', 'last_name', 'gender', 'DOB']] # this selects only the required column from the extracted source data.
df_Customer = df_Customer.drop_duplicates() # This is dropping duplicate rows in the source data.
df_Customer['first_name'] = df_Customer['first_name'].map(lambda x: re.sub(r'\W+', '', x)) # This is removing special characters in source data column.
df_Customer['last_name'] = df_Customer['last_name'].map(lambda x: re.sub(r'\W+', '', x)) # This is removing special characters in source data column.

#This function calculates the Age of a customer based on his Date of Birth
def age(born):
    born = datetime.strptime(born, "%d-%m-%Y").date()
    today = date.today()
    return today.year - born.year - ((today.month, 
                                      today.day) < (born.month, 
                                                    born.day))
df_Customer['Age'] = df_Customer['DOB'].apply(age)

df_Customer['DOB'] = df_Customer['DOB'].astype('datetime64[ns]') # This is converting a String column to date.


#This is adding a column Agerange by bucketing the age column data in 6 age ranges.
bins = [18, 30, 40, 50, 60, 70, 120]
labels = ['18-29', '30-39', '40-49', '50-59', '60-69', '70+']
df_Customer['Agerange'] = pd.cut(df_Customer.Age, bins, labels = labels,include_lowest = True)
            
##### ET Ends here and from here the SCD1 and loading starts ##############

cust_df=pd.read_sql_query('select * from Customer',engine)
cust_src=df_Customer[['customer_id', 'first_name', 'last_name', 'gender', 'DOB', 'Age', 'Agerange']]
#rename columns of src dataset
cust_src.rename(columns={'customer_id':'customer_id_SRC', 'first_name':'first_name_SRC', 'last_name':'last_name_SRC', 'gender':'gender_SRC', 'DOB':'DOB_SRC' , 'Age':'Age_SRC' , 'Agerange':'Agerange_SRC'},inplace=True)
#rename columns of target empty dataframe
cust_tgt=cust_df[['customer_id', 'first_name', 'last_name', 'gender', 'DOB', 'Age', 'Agerange']]
cust_tgt.rename(columns={'customer_id':'customer_id_TGT', 'first_name':'first_name_TGT', 'last_name':'last_name_TGT', 'gender':'gender_TGT', 'DOB':'DOB_TGT', 'Age':'Age_TGT','Agerange':'Agerange_TGT'},inplace=True)
#merge datasets
cust_joined_df=pd.merge(cust_src,cust_tgt,left_on='customer_id_SRC',right_on='customer_id_TGT',how='left')

#Insert Flag
cust_joined_df['INS_FLAG']=cust_joined_df[['customer_id_SRC','customer_id_TGT', 'first_name_SRC', 'first_name_TGT' , 'last_name_SRC', 'last_name_TGT', 'gender_SRC', 'gender_TGT', 'DOB_SRC','DOB_TGT', 'Age_SRC','Age_TGT','Agerange_SRC','Agerange_TGT']].apply(lambda x:
  'I' if pd.isnull(x[1]) else 'N', axis=1)
#Update Flag
cust_joined_df['UPD_FLAG']=cust_joined_df[['customer_id_SRC','customer_id_TGT', 'first_name_SRC', 'first_name_TGT' , 'last_name_SRC', 'last_name_TGT', 'gender_SRC', 'gender_TGT', 'DOB_SRC','DOB_TGT', 'Age_SRC','Age_TGT','Agerange_SRC','Agerange_TGT']].apply(lambda x:
  'U' if (x[0]==x[1] and ( (x[2]!=x[3]) or (x[4]!=x[5]) or (x[6]!=x[7]) or (x[8]!=x[9]) or (x[10]!=x[11])or (x[12]!=x[13])  ))
          else 'N', axis=1)

#create seperate dataset for New records

updateany = cust_joined_df.isin(['U']).any().any()
insertany = cust_joined_df.isin(['I']).any().any()
#print(insertany)
#print(updateany)

if (updateany == False and insertany == False):
    print('No Changes found in and made for  Customer table')
elif(updateany == True and insertany == False):
    # #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['customer_id_SRC' , 'first_name_SRC', 'last_name_SRC' , 'gender_SRC','DOB_SRC' , 'Age_SRC','Agerange_SRC']]
    upd_df.rename(columns={'customer_id_SRC':'customer_id','first_name_SRC':'first_name','last_name_SRC':'last_name','gender_SRC':'gender', 'DOB_SRC':'DOB' , 'Age_SRC':'Age', 'Agerange_SRC':'Agerange'},inplace=True)
    # print(upd_df.head(5))
    # #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('Customer', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'first_name':row.first_name , 'last_name':row.last_name , 'gender':row.gender , 'DOB':row.DOB, 'Age':row.Age, 'Agerange':row.Agerange})\
    		.where (sqla.and_(datatable.c.customer_id==row.customer_id))
    session.execute(upd)
    session.commit()
elif(updateany == False and insertany == True):
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['customer_id_SRC' , 'first_name_SRC', 'last_name_SRC' , 'gender_SRC','DOB_SRC','Age_SRC','Agerange_SRC']]
    ins_upd.rename(columns={'customer_id_SRC':'customer_id','first_name_SRC':'first_name','last_name_SRC':'last_name','gender_SRC':'gender', 'DOB_SRC':'DOB','Age_SRC':'Age', 'Agerange_SRC':'Agerange'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('Customer', con = engine, if_exists = 'append', index=False)
    
else:
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['customer_id_SRC' , 'first_name_SRC', 'last_name_SRC' , 'gender_SRC','DOB_SRC','Age_SRC','Agerange_SRC']]
    ins_upd.rename(columns={'customer_id_SRC':'customer_id','first_name_SRC':'first_name','last_name_SRC':'last_name','gender_SRC':'gender', 'DOB_SRC':'DOB','Age_SRC':'Age', 'Agerange_SRC':'Agerange'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('Customer', con = engine, if_exists = 'append', index=False)
    
        #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['customer_id_SRC' , 'first_name_SRC', 'last_name_SRC' , 'gender_SRC','DOB_SRC','Age_SRC','Agerange_SRC']]
    upd_df.rename(columns={'customer_id_SRC':'customer_id','first_name_SRC':'first_name','last_name_SRC':'last_name','gender_SRC':'gender', 'DOB_SRC':'DOB','Age_SRC':'Age', 'Agerange_SRC':'Agerange'},inplace=True)
    # print(upd_df.head(5))
    
    #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('Customer', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'first_name':row.first_name , 'last_name':row.last_name , 'gender':row.gender , 'DOB':row.DOB, 'Age':row.Age, 'Agerange':row.Agerange})\
    		.where (sqla.and_(datatable.c.customer_id==row.customer_id))
    session.execute(upd)
    session.commit()
    
################## For Address Table SCD1 Implementation #####################
# df_Address.to_sql('Address', con = engine, if_exists = 'append', index=False)
Address_path = os.path.abspath('Address.csv')
df_Address = pd.read_csv(Address_path)
df_Address = df_Address.drop_duplicates() # This is dropping duplicate rows in the source data.

cust_df=pd.read_sql_query('select * from Address',engine)
cust_src=df_Address[['ADDRESSID', 'CITY', 'COUNTRY', 'REGION', 'POSTALCODE']]
#rename columns of src dataset
cust_src.rename(columns={'ADDRESSID':'ADDRESSID_SRC', 'CITY':'CITY_SRC', 'COUNTRY':'COUNTRY_SRC', 'REGION':'REGION_SRC', 'POSTALCODE':'POSTALCODE_SRC'},inplace=True)

#rename columns of target empty dataframe
cust_tgt=cust_df[['ADDRESSID', 'CITY', 'COUNTRY', 'REGION', 'POSTALCODE']]
cust_tgt.rename(columns={'ADDRESSID':'ADDRESSID_TGT', 'CITY':'CITY_TGT', 'COUNTRY':'COUNTRY_TGT', 'REGION':'REGION_TGT', 'POSTALCODE':'POSTALCODE_TGT'},inplace=True)
 
 #merge datasets
cust_joined_df=pd.merge(cust_src,cust_tgt,left_on='ADDRESSID_SRC',right_on='ADDRESSID_TGT',how='left')
# print(cust_joined_df)

#Insert Flag
cust_joined_df['INS_FLAG']=cust_joined_df[['ADDRESSID_SRC','ADDRESSID_TGT', 'CITY_SRC', 'CITY_TGT' , 'COUNTRY_SRC', 'COUNTRY_TGT', 'REGION_SRC', 'REGION_TGT', 'POSTALCODE_SRC','POSTALCODE_TGT']].apply(lambda x:
  'I' if pd.isnull(x[1]) else 'N', axis=1)
#Update Flag
cust_joined_df['UPD_FLAG']=cust_joined_df[['ADDRESSID_SRC','ADDRESSID_TGT', 'CITY_SRC', 'CITY_TGT' , 'COUNTRY_SRC', 'COUNTRY_TGT', 'REGION_SRC', 'REGION_TGT', 'POSTALCODE_SRC','POSTALCODE_TGT']].apply(lambda x:
  'U' if (x[0]==x[1] and ( (x[2]!=x[3]) or (x[4]!=x[5]) or (x[6]!=x[7]) or (x[8]!=x[9]) ))
          else 'N', axis=1)

#create seperate dataset for New records

updateany = cust_joined_df.isin(['U']).any().any()
insertany = cust_joined_df.isin(['I']).any().any()
#print(insertany)
#print(updateany)


if (updateany == False and insertany == False):
    print('No Changes found in and made for  Address table')
elif(updateany == True and insertany == False):
    # #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['ADDRESSID_SRC' , 'CITY_SRC', 'COUNTRY_SRC' , 'REGION_SRC','POSTALCODE_SRC']]
    upd_df.rename(columns={'ADDRESSID_SRC':'ADDRESSID','CITY_SRC':'CITY','COUNTRY_SRC':'COUNTRY','REGION_SRC':'REGION', 'POSTALCODE_SRC':'POSTALCODE'},inplace=True)

    # #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('Address', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'CITY':row.CITY , 'COUNTRY':row.COUNTRY , 'REGION':row.REGION , 'POSTALCODE':row.POSTALCODE})\
    		.where (sqla.and_(datatable.c.ADDRESSID==row.ADDRESSID))
    session.execute(upd)
    session.commit()
elif(updateany == False and insertany == True):
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['ADDRESSID_SRC' , 'CITY_SRC', 'COUNTRY_SRC' , 'REGION_SRC','POSTALCODE_SRC']]
    ins_upd.rename(columns={'ADDRESSID_SRC':'ADDRESSID','CITY_SRC':'CITY','COUNTRY_SRC':'COUNTRY','REGION_SRC':'REGION', 'POSTALCODE_SRC':'POSTALCODE'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('Address', con = engine, if_exists = 'append', index=False)
    
else:
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['ADDRESSID_SRC' , 'CITY_SRC', 'COUNTRY_SRC' , 'REGION_SRC','POSTALCODE_SRC']]
    ins_upd.rename(columns={'ADDRESSID_SRC':'ADDRESSID','CITY_SRC':'CITY','COUNTRY_SRC':'COUNTRY','REGION_SRC':'REGION', 'POSTALCODE_SRC':'POSTALCODE'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('Address', con = engine, if_exists = 'append', index=False)
    
        #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['ADDRESSID_SRC' , 'CITY_SRC', 'COUNTRY_SRC' , 'REGION_SRC','POSTALCODE_SRC']]
    upd_df.rename(columns={'ADDRESSID_SRC':'ADDRESSID','CITY_SRC':'CITY','COUNTRY_SRC':'COUNTRY','REGION_SRC':'REGION', 'POSTALCODE_SRC':'POSTALCODE'},inplace=True)
    # print(upd_df.head(5))
    
        #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('Address', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'CITY':row.CITY , 'COUNTRY':row.COUNTRY , 'REGION':row.REGION , 'POSTALCODE':row.POSTALCODE})\
    		.where (sqla.and_(datatable.c.ADDRESSID==row.ADDRESSID))
    session.execute(upd)
    session.commit()


################## For BusinessPartner Table SCD1 Implementation #####################

BusinessPartner_path = os.path.abspath('BusinessPartner.csv')
df_BusinessPartner = pd.read_csv(BusinessPartner_path)
df_BusinessPartner = df_BusinessPartner.drop_duplicates() # This is dropping duplicate rows in the source data.

# df_BusinessPartner.to_sql('BusinessPartner', con = engine, if_exists = 'append', index=False)
cust_df=pd.read_sql_query('select * from BusinessPartner',engine)

cust_src=df_BusinessPartner[['PARTNERID', 'EMAILADDRESS', 'ADDRESSID', 'COMPANYNAME']]
#rename columns of src dataset
cust_src.rename(columns={'PARTNERID':'PARTNERID_SRC', 'EMAILADDRESS':'EMAILADDRESS_SRC', 'ADDRESSID':'ADDRESSID_SRC', 'COMPANYNAME':'COMPANYNAME_SRC'},inplace=True)

#rename columns of target empty dataframe
cust_tgt=cust_df[['PARTNERID', 'EMAILADDRESS', 'ADDRESSID', 'COMPANYNAME']]
cust_tgt.rename(columns={'PARTNERID':'PARTNERID_TGT', 'EMAILADDRESS':'EMAILADDRESS_TGT', 'ADDRESSID':'ADDRESSID_TGT', 'COMPANYNAME':'COMPANYNAME_TGT'},inplace=True)
 
 #merge datasets
cust_joined_df=pd.merge(cust_src,cust_tgt,left_on='PARTNERID_SRC',right_on='PARTNERID_TGT',how='left')
# print(cust_joined_df)

#Insert Flag
cust_joined_df['INS_FLAG']=cust_joined_df[['PARTNERID_SRC','PARTNERID_TGT', 'EMAILADDRESS_SRC', 'EMAILADDRESS_TGT' , 'ADDRESSID_SRC', 'ADDRESSID_TGT', 'COMPANYNAME_SRC', 'COMPANYNAME_TGT']].apply(lambda x:
  'I' if pd.isnull(x[1]) else 'N', axis=1)
#Update Flag
cust_joined_df['UPD_FLAG']=cust_joined_df[['PARTNERID_SRC','PARTNERID_TGT', 'EMAILADDRESS_SRC', 'EMAILADDRESS_TGT' , 'ADDRESSID_SRC', 'ADDRESSID_TGT', 'COMPANYNAME_SRC', 'COMPANYNAME_TGT']].apply(lambda x:
  'U' if (x[0]==x[1] and ( (x[2]!=x[3]) or (x[4]!=x[5]) or (x[6]!=x[7]) ))
          else 'N', axis=1)
#create seperate dataset for New records

updateany = cust_joined_df.isin(['U']).any().any()
insertany = cust_joined_df.isin(['I']).any().any()
#print(insertany)
#print(updateany)


if (updateany == False and insertany == False):
    print('No Changes found in and made for  BusinessPartner table')
elif(updateany == True and insertany == False):
    # #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['PARTNERID_SRC' , 'EMAILADDRESS_SRC', 'ADDRESSID_SRC' , 'COMPANYNAME_SRC']]
    upd_df.rename(columns={'PARTNERID_SRC':'PARTNERID','EMAILADDRESS_SRC':'EMAILADDRESS','ADDRESSID_SRC':'ADDRESSID','COMPANYNAME_SRC':'COMPANYNAME'},inplace=True)
    # print(upd_df.head(5))
    # #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('BusinessPartner', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'EMAILADDRESS':row.EMAILADDRESS , 'ADDRESSID':row.ADDRESSID , 'COMPANYNAME':row.COMPANYNAME })\
    		.where (sqla.and_(datatable.c.PARTNERID==row.PARTNERID))
    session.execute(upd)
    session.commit()
elif(updateany == False and insertany == True):
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['PARTNERID_SRC' , 'EMAILADDRESS_SRC', 'ADDRESSID_SRC' , 'COMPANYNAME_SRC']]
    ins_upd.rename(columns={'PARTNERID_SRC':'PARTNERID','EMAILADDRESS_SRC':'EMAILADDRESS','ADDRESSID_SRC':'ADDRESSID','COMPANYNAME_SRC':'COMPANYNAME'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('BusinessPartner', con = engine, if_exists = 'append', index=False)
    
else:
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['PARTNERID_SRC' , 'EMAILADDRESS_SRC', 'ADDRESSID_SRC' , 'COMPANYNAME_SRC']]
    ins_upd.rename(columns={'PARTNERID_SRC':'PARTNERID','EMAILADDRESS_SRC':'EMAILADDRESS','ADDRESSID_SRC':'ADDRESSID','COMPANYNAME_SRC':'COMPANYNAME'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('BusinessPartner', con = engine, if_exists = 'append', index=False)
    
        #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['PARTNERID_SRC' , 'EMAILADDRESS_SRC', 'ADDRESSID_SRC' , 'COMPANYNAME_SRC']]
    upd_df.rename(columns={'PARTNERID_SRC':'PARTNERID','EMAILADDRESS_SRC':'EMAILADDRESS','ADDRESSID_SRC':'ADDRESSID','COMPANYNAME_SRC':'COMPANYNAME', },inplace=True)
    # print(upd_df.head(5))
    
        #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('BusinessPartner', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'EMAILADDRESS':row.EMAILADDRESS , 'ADDRESSID':row.ADDRESSID , 'COMPANYNAME':row.COMPANYNAME })\
    		.where (sqla.and_(datatable.c.PARTNERID==row.PARTNERID))
    session.execute(upd)
    session.commit()
    
################## For ProductCategory Table SCD1 Implementation #####################

ProductCategory_path = os.path.abspath('ProductCategory.csv')
df_ProductCategory = pd.read_csv(ProductCategory_path)
df_ProductCategory = df_ProductCategory.drop_duplicates() # This is dropping duplicate rows in the source data.

# df_BusinessPartner.to_sql('ProductCategory', con = engine, if_exists = 'append', index=False)
cust_df=pd.read_sql_query('select * from ProductCategory',engine)
cust_src=df_ProductCategory[['PRODCATEGORYID', 'PRODCATEGORYNAME']]
#rename columns of src dataset
cust_src.rename(columns={'PRODCATEGORYID':'PRODCATEGORYID_SRC', 'PRODCATEGORYNAME':'PRODCATEGORYNAME_SRC'},inplace=True)

#rename columns of target empty dataframe
cust_tgt=cust_df[['PRODCATEGORYID', 'PRODCATEGORYNAME']]
cust_tgt.rename(columns={'PRODCATEGORYID':'PRODCATEGORYID_TGT', 'PRODCATEGORYNAME':'PRODCATEGORYNAME_TGT'},inplace=True)
 
 #merge datasets
cust_joined_df=pd.merge(cust_src,cust_tgt,left_on='PRODCATEGORYID_SRC',right_on='PRODCATEGORYID_TGT',how='left')
# print(cust_joined_df)

#Insert Flag
cust_joined_df['INS_FLAG']=cust_joined_df[['PRODCATEGORYID_SRC','PRODCATEGORYID_TGT', 'PRODCATEGORYNAME_SRC', 'PRODCATEGORYNAME_TGT' ]].apply(lambda x:
  'I' if pd.isnull(x[1]) else 'N', axis=1)
#Update Flag
cust_joined_df['UPD_FLAG']=cust_joined_df[['PRODCATEGORYID_SRC','PRODCATEGORYID_TGT', 'PRODCATEGORYNAME_SRC', 'PRODCATEGORYNAME_TGT' ]].apply(lambda x:
  'U' if (x[0]==x[1] and ( (x[2]!=x[3])))
          else 'N', axis=1)
#create seperate dataset for New records
updateany = cust_joined_df.isin(['U']).any().any()
insertany = cust_joined_df.isin(['I']).any().any()
#print(insertany)
#print(updateany)


if (updateany == False and insertany == False):
    print('No Changes found in and made for  ProductCategory table')
elif(updateany == True and insertany == False):
    # #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['PRODCATEGORYID_SRC' , 'PRODCATEGORYNAME_SRC']]
    upd_df.rename(columns={'PRODCATEGORYID_SRC':'PRODCATEGORYID','PRODCATEGORYNAME_SRC':'PRODCATEGORYNAME',},inplace=True)
    # #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('ProductCategory', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'PRODCATEGORYNAME':row.PRODCATEGORYNAME })\
    		.where (sqla.and_(datatable.c.PRODCATEGORYID==row.PRODCATEGORYID))
    session.execute(upd)
    session.commit()
elif(updateany == False and insertany == True):
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['PRODCATEGORYID_SRC' , 'PRODCATEGORYNAME_SRC']]
    ins_upd.rename(columns={'PRODCATEGORYID_SRC':'PRODCATEGORYID','PRODCATEGORYNAME_SRC':'PRODCATEGORYNAME'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('ProductCategory', con = engine, if_exists = 'append', index=False)
    
else:
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['PRODCATEGORYID_SRC' , 'PRODCATEGORYNAME_SRC']]
    ins_upd.rename(columns={'PRODCATEGORYID_SRC':'PRODCATEGORYID','PRODCATEGORYNAME_SRC':'PRODCATEGORYNAME'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('ProductCategory', con = engine, if_exists = 'append', index=False)
    
        #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['PRODCATEGORYID_SRC' , 'PRODCATEGORYNAME_SRC']]
    upd_df.rename(columns={'PRODCATEGORYID_SRC':'PRODCATEGORYID','PRODCATEGORYNAME_SRC':'PRODCATEGORYNAME'},inplace=True)
    # print(upd_df.head(5))
    
        #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('ProductCategory', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'PRODCATEGORYNAME':row.PRODCATEGORYNAME })\
    		.where (sqla.and_(datatable.c.PRODCATEGORYID==row.PRODCATEGORYID))
    session.execute(upd)
    session.commit()
    
################## For ProductDetail Table SCD1 Implementation #####################

ProductDetail_path = os.path.abspath('ProductDetail.csv')
df_ProductDetail = pd.read_csv(ProductDetail_path)
df_ProductDetail = df_ProductDetail.drop_duplicates() # This is dropping duplicate rows in the source data.

# df_BusinessPartner.to_sql('ProductDetail', con = engine, if_exists = 'append', index=False)
cust_df=pd.read_sql_query('select * from ProductDetail',engine)
# print(cust_df.head(20))

cust_src=df_ProductDetail[['PRODUCTID', 'PRODUCT_NAME']]
#rename columns of src dataset
cust_src.rename(columns={'PRODUCTID':'PRODUCTID_SRC', 'PRODUCT_NAME':'PRODUCT_NAME_SRC'},inplace=True)

#rename columns of target empty dataframe
cust_tgt=cust_df[['PRODUCTID', 'PRODUCT_NAME']]
cust_tgt.rename(columns={'PRODUCTID':'PRODUCTID_TGT', 'PRODUCT_NAME':'PRODUCT_NAME_TGT'},inplace=True)
 
 #merge datasets
cust_joined_df=pd.merge(cust_src,cust_tgt,left_on='PRODUCTID_SRC',right_on='PRODUCTID_TGT',how='left')
# print(cust_joined_df)

#Insert Flag
cust_joined_df['INS_FLAG']=cust_joined_df[['PRODUCTID_SRC','PRODUCTID_TGT', 'PRODUCT_NAME_SRC', 'PRODUCT_NAME_TGT' ]].apply(lambda x:
  'I' if pd.isnull(x[1]) else 'N', axis=1)
#Update Flag
cust_joined_df['UPD_FLAG']=cust_joined_df[['PRODUCTID_SRC','PRODUCTID_TGT', 'PRODUCT_NAME_SRC', 'PRODUCT_NAME_TGT' ]].apply(lambda x:
  'U' if (x[0]==x[1] and ( (x[2]!=x[3])))
          else 'N', axis=1)

#create seperate dataset for New records

updateany = cust_joined_df.isin(['U']).any().any()
insertany = cust_joined_df.isin(['I']).any().any()
#print(insertany)
#print(updateany)


if (updateany == False and insertany == False):
    print('No Changes found in and made for  ProductDetail table')
elif(updateany == True and insertany == False):
    # #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['PRODUCTID_SRC' , 'PRODUCT_NAME_SRC']]
    upd_df.rename(columns={'PRODUCTID_SRC':'PRODUCTID','PRODUCT_NAME_SRC':'PRODUCT_NAME',},inplace=True)
    # #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('ProductDetail', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'PRODUCT_NAME':row.PRODUCT_NAME })\
    		.where (sqla.and_(datatable.c.PRODUCTID==row.PRODUCTID))
    session.execute(upd)
    session.commit()
elif(updateany == False and insertany == True):
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['PRODUCTID_SRC' , 'PRODUCT_NAME_SRC']]
    ins_upd.rename(columns={'PRODUCTID_SRC':'PRODUCTID','PRODUCT_NAME_SRC':'PRODUCT_NAME'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('ProductDetail', con = engine, if_exists = 'append', index=False)
    
else:
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['PRODUCTID_SRC' , 'PRODUCT_NAME_SRC']]
    ins_upd.rename(columns={'PRODUCTID_SRC':'PRODUCTID','PRODUCT_NAME_SRC':'PRODUCT_NAME'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('ProductDetail', con = engine, if_exists = 'append', index=False)
    
        #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['PRODUCTID_SRC' , 'PRODUCT_NAME_SRC']]
    upd_df.rename(columns={'PRODUCTID_SRC':'PRODUCTID','PRODUCT_NAME_SRC':'PRODUCT_NAME'},inplace=True)
    # print(upd_df.head(5))
    
        #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('ProductDetail', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'PRODUCT_NAME':row.PRODUCT_NAME })\
    		.where (sqla.and_(datatable.c.PRODUCTID==row.PRODUCTID))
    session.execute(upd)
    session.commit()
    
    
################## For Store Table SCD1 Implementation #####################

Store_path = os.path.abspath('Store.csv')
df_Store = pd.read_csv(Store_path)
df_Store = df_Store.drop_duplicates() # This is dropping duplicate rows in the source data.

# df_Store.to_sql('Store', con = engine, if_exists = 'append', index=False)
cust_df=pd.read_sql_query('select * from Store',engine)
cust_src=df_Store[['StoreID', 'manager', 'AddressID', 'phone']]
#rename columns of src dataset
cust_src.rename(columns={'StoreID':'StoreID_SRC', 'manager':'manager_SRC', 'AddressID':'AddressID_SRC', 'phone':'phone_SRC'},inplace=True)

#rename columns of target empty dataframe
cust_tgt=cust_df[['StoreID', 'manager', 'AddressID', 'phone']]
cust_tgt.rename(columns={'StoreID':'StoreID_TGT', 'manager':'manager_TGT', 'AddressID':'AddressID_TGT', 'phone':'phone_TGT'},inplace=True)
 
 #merge datasets
cust_joined_df=pd.merge(cust_src,cust_tgt,left_on='StoreID_SRC',right_on='StoreID_TGT',how='left')
# print(cust_joined_df)



#Insert Flag
cust_joined_df['INS_FLAG']=cust_joined_df[['StoreID_SRC','StoreID_TGT', 'manager_SRC', 'manager_TGT' , 'AddressID_SRC', 'AddressID_TGT', 'phone_SRC', 'phone_TGT']].apply(lambda x:
  'I' if pd.isnull(x[1]) else 'N', axis=1)
#Update Flag
cust_joined_df['UPD_FLAG']=cust_joined_df[['StoreID_SRC','StoreID_TGT', 'manager_SRC', 'manager_TGT' , 'AddressID_SRC', 'AddressID_TGT', 'phone_SRC', 'phone_TGT']].apply(lambda x:
  'U' if (x[0]==x[1] and ( (x[2]!=x[3]) or (x[4]!=x[5]) or (x[6]!=x[7]) ))
          else 'N', axis=1)
#create seperate dataset for New records

updateany = cust_joined_df.isin(['U']).any().any()
insertany = cust_joined_df.isin(['I']).any().any()
#print(insertany)
#print(updateany)


if (updateany == False and insertany == False):
    print('No Changes found in and made for  Store table')
elif(updateany == True and insertany == False):
    # #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['StoreID_SRC' , 'manager_SRC', 'AddressID_SRC' , 'phone_SRC']]
    upd_df.rename(columns={'StoreID_SRC':'StoreID','manager_SRC':'manager','AddressID_SRC':'AddressID','phone_SRC':'phone'},inplace=True)
    # print(upd_df.head(5))
    # #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('Store', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'manager':row.manager , 'AddressID':row.AddressID , 'phone':row.phone })\
    		.where (sqla.and_(datatable.c.StoreID==row.StoreID))
    session.execute(upd)
    session.commit()
elif(updateany == False and insertany == True):
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['StoreID_SRC' , 'manager_SRC', 'AddressID_SRC' , 'phone_SRC']]
    ins_upd.rename(columns={'StoreID_SRC':'StoreID','manager_SRC':'manager','AddressID_SRC':'AddressID','phone_SRC':'phone'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('Store', con = engine, if_exists = 'append', index=False)
    
else:
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['StoreID_SRC' , 'manager_SRC', 'AddressID_SRC' , 'phone_SRC']]
    ins_upd.rename(columns={'StoreID_SRC':'StoreID','manager_SRC':'manager','AddressID_SRC':'AddressID','phone_SRC':'phone'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('Store', con = engine, if_exists = 'append', index=False)
    
        #Create seperate dataset for Updated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['StoreID_SRC' , 'manager_SRC', 'AddressID_SRC' , 'phone_SRC']]
    upd_df.rename(columns={'StoreID_SRC':'StoreID','manager_SRC':'manager','AddressID_SRC':'AddressID','phone_SRC':'phone', },inplace=True)
   
        #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('Store', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'manager':row.manager , 'AddressID':row.AddressID , 'phone':row.phone })\
    		.where (sqla.and_(datatable.c.StoreID==row.StoreID))
    session.execute(upd)
    session.commit()

################## For SalesOrder Table SCD1 Implementation #####################

SalesOrder_path = os.path.abspath('SalesOrder.csv')
df_SalesOrder = pd.read_csv(SalesOrder_path)
df_SalesOrder = df_SalesOrder.drop_duplicates() # This is dropping duplicate rows in the source data.

df_SalesOrder['Date'] = df_SalesOrder['Date'].astype('datetime64[ns]') # This is converting a String column to date.

# df_SalesOrder.to_sql('SalesOrder', con = engine, if_exists = 'append', index=False)
cust_df=pd.read_sql_query('select * from SalesOrder',engine)
cust_src=df_SalesOrder[['SalesOrderID',  'SALESORG', 'GROSSAMOUNT', 'Ordertype', 'StoreID', 'Date', 'RATING','customer_id']]
#rename columns of src dataset
cust_src.rename(columns={'SalesOrderID':'SalesOrderID_SRC', 'SALESORG':'SALESORG_SRC', 'GROSSAMOUNT':'GROSSAMOUNT_SRC' , 'Ordertype': 'Ordertype_SRC', 'StoreID': 'StoreID_SRC', 'Date': 'Date_SRC', 'RATING': 'RATING_SRC','customer_id': 'customer_id_SRC'},inplace=True)

#rename columns of target empty dataframe
cust_tgt=cust_df[['SalesOrderID', 'SALESORG', 'GROSSAMOUNT', 'Ordertype','StoreID', 'Date', 'RATING','customer_id']]
cust_tgt.rename(columns={'SalesOrderID':'SalesOrderID_TGT', 'SALESORG':'SALESORG_TGT', 'GROSSAMOUNT':'GROSSAMOUNT_TGT', 'Ordertype': 'Ordertype_TGT', 'StoreID': 'StoreID_TGT', 'Date': 'Date_TGT', 'RATING': 'RATING_TGT','customer_id': 'customer_id_TGT'},inplace=True)
 
 #merge datasets
cust_joined_df=pd.merge(cust_src,cust_tgt,left_on='SalesOrderID_SRC',right_on='SalesOrderID_TGT',how='left')

#Insert Flag
cust_joined_df['INS_FLAG']=cust_joined_df[['SalesOrderID_SRC','SalesOrderID_TGT', 'SALESORG_SRC', 'SALESORG_TGT', 'GROSSAMOUNT_SRC', 'GROSSAMOUNT_TGT', 'Ordertype_SRC', 'Ordertype_TGT', 'StoreID_SRC', 'StoreID_TGT', 'Date_SRC', 'Date_TGT', 'RATING_SRC', 'RATING_TGT', 'customer_id_SRC', 'customer_id_TGT' ]].apply(lambda x:
  'I' if pd.isnull(x[1]) else 'N', axis=1)
#UpDate Flag
cust_joined_df['UPD_FLAG']=cust_joined_df[['SalesOrderID_SRC','SalesOrderID_TGT', 'SALESORG_SRC', 'SALESORG_TGT', 'GROSSAMOUNT_SRC', 'GROSSAMOUNT_TGT', 'Ordertype_SRC', 'Ordertype_TGT', 'StoreID_SRC', 'StoreID_TGT', 'Date_SRC', 'Date_TGT', 'RATING_SRC', 'RATING_TGT', 'customer_id_SRC', 'customer_id_TGT']].apply(lambda x:
  'U' if (x[0]==x[1] and ( (x[2]!=x[3]) or (x[4]!=x[5]) or (x[6]!=x[7])or (x[8]!=x[9]) or  (x[10]!=x[11])or (x[12]!=x[13]) or (x[14]!=x[15]) ))
          else 'N', axis=1)
#create seperate dataset for New records

updateany = cust_joined_df.isin(['U']).any().any()
insertany = cust_joined_df.isin(['I']).any().any()
#print(insertany)
#print(updateany)


if (updateany == False and insertany == False):
    print('No Changes found in and made for  SalesOrder table')
elif(updateany == True and insertany == False):
    # #Create seperate dataset for UpDated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['SalesOrderID_SRC' ,  'SALESORG_SRC' , 'GROSSAMOUNT_SRC', 'Ordertype_SRC', 'StoreID_SRC','Date_SRC','RATING_SRC','customer_id_SRC']]
    upd_df.rename(columns={'SalesOrderID_SRC':'SalesOrderID','SALESORG_SRC':'SALESORG','GROSSAMOUNT_SRC':'GROSSAMOUNT', 'Ordertype_SRC':'Ordertype', 'StoreID_SRC':'StoreID','Date_SRC':'Date','RATING_SRC':'RATING','customer_id_SRC':'customer_id'},inplace=True)
    # #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('SalesOrder', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({ 'SALESORG':row.SALESORG , 'GROSSAMOUNT':row.GROSSAMOUNT,'QUANTITY':row.QUANTITY, 'StoreID':row.StoreID ,'Date':row.Date,'RATING':row.RATING,'customer_id':row.customer_id  })\
    		.where (sqla.and_(datatable.c.SalesOrderID==row.SalesOrderID))
    session.execute(upd)
    session.commit()
elif(updateany == False and insertany == True):
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['SalesOrderID_SRC' ,  'SALESORG_SRC' , 'GROSSAMOUNT_SRC' , 'Ordertype_SRC', 'StoreID_SRC','Date_SRC','RATING_SRC','customer_id_SRC']]
    ins_upd.rename(columns={'SalesOrderID_SRC':'SalesOrderID','SALESORG_SRC':'SALESORG','GROSSAMOUNT_SRC':'GROSSAMOUNT', 'Ordertype_SRC':'Ordertype', 'StoreID_SRC':'StoreID','Date_SRC':'Date','RATING_SRC':'RATING','customer_id_SRC':'customer_id'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('SalesOrder', con = engine, if_exists = 'append', index=False)
    
else:
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['SalesOrderID_SRC' ,  'SALESORG_SRC' , 'GROSSAMOUNT_SRC', 'Ordertype_SRC', 'StoreID_SRC','Date_SRC','RATING_SRC','customer_id_SRC']]
    ins_upd.rename(columns={'SalesOrderID_SRC':'SalesOrderID','SALESORG_SRC':'SALESORG','GROSSAMOUNT_SRC':'GROSSAMOUNT','Ordertype_SRC':'Ordertype', 'StoreID_SRC':'StoreID','Date_SRC':'Date','RATING_SRC':'RATING','customer_id_SRC':'customer_id'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('SalesOrder', con = engine, if_exists = 'append', index=False)
    
        #Create seperate dataset for UpDated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['SalesOrderID_SRC' ,  'SALESORG_SRC' , 'GROSSAMOUNT_SRC', 'Ordertype_SRC', 'StoreID_SRC','Date_SRC','RATING_SRC','customer_id_SRC']]
    upd_df.rename(columns={'SalesOrderID_SRC':'SalesOrderID','SALESORG_SRC':'SALESORG','GROSSAMOUNT_SRC':'GROSSAMOUNT','Ordertype_SRC':'Ordertype', 'StoreID_SRC':'StoreID','Date_SRC':'Date','RATING_SRC':'RATING','customer_id_SRC':'customer_id' },inplace=True)
   
        #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('SalesOrder', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'SALESORG':row.SALESORG , 'GROSSAMOUNT':row.GROSSAMOUNT,'Ordertype':row.Ordertype, 'StoreID':row.StoreID ,'Date':row.Date,'RATING':row.RATING,'customer_id':row.customer_id })\
    		.where (sqla.and_(datatable.c.SalesOrderID==row.SalesOrderID))
    session.execute(upd)
    session.commit()
    
################## For Product Table SCD2 Implementation #####################

Product_path = os.path.abspath('Product.csv')
df_Product = pd.read_csv(Product_path)
df_Product = df_Product.drop_duplicates() # This is dropping duplicate rows in the source data.

# df_Product.to_sql('Product', con = engine, if_exists = 'append', index=False)
cust_df=pd.read_sql_query('select * from Product where current_flag = 1',engine)
cust_src=df_Product[[ 'PRODUCTID', 'PRODCATEGORYID', 'PARTNERID', 'PRICE']]
#rename columns of src dataset
cust_src.rename(columns={ 'PRODUCTID':'PRODUCTID_SRC', 'PRODCATEGORYID':'PRODCATEGORYID_SRC', 'PARTNERID':'PARTNERID_SRC' , 'PRICE': 'PRICE_SRC'},inplace=True)

#rename columns of target empty dataframe
cust_tgt=cust_df[['PRODUCTID', 'PRODCATEGORYID', 'PARTNERID', 'PRICE']]
cust_tgt.rename(columns={ 'PRODUCTID':'PRODUCTID_TGT', 'PRODCATEGORYID':'PRODCATEGORYID_TGT', 'PARTNERID':'PARTNERID_TGT', 'PRICE': 'PRICE_TGT'},inplace=True)
 
 #merge datasets
cust_joined_df=pd.merge(cust_src,cust_tgt,left_on='PRODUCTID_SRC',right_on='PRODUCTID_TGT',how='left')

#Insert Flag
cust_joined_df['INS_FLAG']=cust_joined_df[[ 'PRODUCTID_SRC', 'PRODUCTID_TGT' , 'PRODCATEGORYID_SRC', 'PRODCATEGORYID_TGT', 'PARTNERID_SRC', 'PARTNERID_TGT', 'PRICE_SRC', 'PRICE_TGT' ]].apply(lambda x:
  'I' if pd.isnull(x[1]) else 'N', axis=1)
#UpDate Flag
cust_joined_df['INS_UPD_FLAG']=cust_joined_df[[ 'PRODUCTID_SRC', 'PRODUCTID_TGT' , 'PRODCATEGORYID_SRC', 'PRODCATEGORYID_TGT', 'PARTNERID_SRC', 'PARTNERID_TGT', 'PRICE_SRC', 'PRICE_TGT']].apply(lambda x:
  'UI' if (x[0]==x[1] and ( (x[2]!=x[3]) or (x[4]!=x[5]) or (x[6]!=x[7]) ))
          else 'N', axis=1)
#create seperate dataset for New records

updateany = cust_joined_df.isin(['UI']).any().any()
# print(updateany)
insertany = cust_joined_df.isin(['I']).any().any()
# print(insertany)

if (updateany == False and insertany == False):
    print('No Changes found in and made for  Product table')
elif(updateany == True and insertany == False):
    # #Create seperate dataset for UpDated records
    upd_rec=cust_joined_df[cust_joined_df['INS_UPD_FLAG']=='UI']
    upd_df=upd_rec[[ 'PRODUCTID_SRC', 'PRODCATEGORYID_SRC' , 'PARTNERID_SRC', 'PRICE_SRC']]
    upd_df['CURRENT_FLAG']=1
    upd_df.rename(columns={'PRODUCTID_SRC':'PRODUCTID','PRODCATEGORYID_SRC':'PRODCATEGORYID','PARTNERID_SRC':'PARTNERID', 'PRICE_SRC':'PRICE'},inplace=True)
    # print(upd_df.head(5))
    # #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('Product', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'CURRENT_FLAG':0, 'expire_timestamp': datetime.now()})\
    		.where (sqla.and_(datatable.c.PRODUCTID==row.PRODUCTID))
    session.execute(upd)
    session.commit()
    upd_df.to_sql('Product', con = engine, if_exists = 'append', index=False)

elif(updateany == False and insertany == True):
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[[ 'PRODUCTID_SRC', 'PRODCATEGORYID_SRC' , 'PARTNERID_SRC' , 'PRICE_SRC']]
    ins_upd.rename(columns={'PRODUCTID_SRC':'PRODUCTID','PRODCATEGORYID_SRC':'PRODCATEGORYID','PARTNERID_SRC':'PARTNERID','PRICE_SRC':'PRICE'},inplace=True)
    ins_upd['CURRENT_FLAG']=1   
    # print(ins_upd.head(5))
    #Insert new records to target table:
    ins_upd.to_sql('Product', con = engine, if_exists = 'append', index=False)
    
else:
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[[ 'PRODUCTID_SRC', 'PRODCATEGORYID_SRC' , 'PARTNERID_SRC', 'PRICE_SRC']]
    ins_upd.rename(columns={'PRODUCTID_SRC':'PRODUCTID','PRODCATEGORYID_SRC':'PRODCATEGORYID','PARTNERID_SRC':'PARTNERID','PRICE_SRC':'PRICE'},inplace=True)
    ins_upd['CURRENT_FLAG']=1   
    # print(ins_upd.head(5))
    #Insert new records to target table:
    ins_upd.to_sql('Product', con = engine, if_exists = 'append', index=False)
    
    upd_rec=cust_joined_df[cust_joined_df['INS_UPD_FLAG']=='UI']
    upd_df=upd_rec[[ 'PRODUCTID_SRC', 'PRODCATEGORYID_SRC' , 'PARTNERID_SRC', 'PRICE_SRC']]
    upd_df['CURRENT_FLAG']=1
    upd_df.rename(columns={'PRODUCTID_SRC':'PRODUCTID','PRODCATEGORYID_SRC':'PRODCATEGORYID','PARTNERID_SRC':'PARTNERID', 'PRICE_SRC':'PRICE'},inplace=True)
    # print(upd_df.head(5))
    # #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('Product', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'CURRENT_FLAG':0, 'expire_timestamp': datetime.now()})\
    		.where (sqla.and_(datatable.c.PRODUCTID==row.PRODUCTID))
    session.execute(upd)
    session.commit()
    upd_df.to_sql('Product', con = engine, if_exists = 'append', index=False)
    
    
################## For SalesOrderItems Table SCD1 Implementation #####################

SalesOrderItems_path = os.path.abspath('SalesOrderItems.csv')
df_SalesOrderItems = pd.read_csv(SalesOrderItems_path)
df_SalesOrderItems = df_SalesOrderItems.drop_duplicates() # This is dropping duplicate rows in the source data.

# df_SalesOrderItems.to_sql('SalesOrderItems', con = engine, if_exists = 'append', index=False)
cust_df=pd.read_sql_query('select * from SalesOrderItems',engine)
cust_src=df_SalesOrderItems[['SalesOrderItemsID', 'PRODUCTID', 'SalesOrderID', 'GROSSAMOUNT', 'QUANTITY']]
#rename columns of src dataset
cust_src.rename(columns={'SalesOrderItemsID':'SalesOrderItemsID_SRC', 'PRODUCTID':'PRODUCTID_SRC', 'SalesOrderID':'SalesOrderID_SRC', 'GROSSAMOUNT':'GROSSAMOUNT_SRC' , 'QUANTITY': 'QUANTITY_SRC'},inplace=True)

#rename columns of target empty dataframe
cust_tgt=cust_df[['SalesOrderItemsID', 'PRODUCTID', 'SalesOrderID', 'GROSSAMOUNT', 'QUANTITY']]
cust_tgt.rename(columns={'SalesOrderItemsID':'SalesOrderItemsID_TGT', 'PRODUCTID':'PRODUCTID_TGT', 'SalesOrderID':'SalesOrderID_TGT', 'GROSSAMOUNT':'GROSSAMOUNT_TGT', 'QUANTITY': 'QUANTITY_TGT'},inplace=True)
 
 #merge datasets
cust_joined_df=pd.merge(cust_src,cust_tgt,left_on='SalesOrderItemsID_SRC',right_on='SalesOrderItemsID_TGT',how='left')

#Insert Flag
cust_joined_df['INS_FLAG']=cust_joined_df[['SalesOrderItemsID_SRC','SalesOrderItemsID_TGT', 'PRODUCTID_SRC', 'PRODUCTID_TGT' , 'SalesOrderID_SRC', 'SalesOrderID_TGT', 'GROSSAMOUNT_SRC', 'GROSSAMOUNT_TGT', 'QUANTITY_SRC', 'QUANTITY_TGT' ]].apply(lambda x:
  'I' if pd.isnull(x[1]) else 'N', axis=1)
#UpDate Flag
cust_joined_df['UPD_FLAG']=cust_joined_df[['SalesOrderItemsID_SRC','SalesOrderItemsID_TGT', 'PRODUCTID_SRC', 'PRODUCTID_TGT' , 'SalesOrderID_SRC', 'SalesOrderID_TGT', 'GROSSAMOUNT_SRC', 'GROSSAMOUNT_TGT', 'QUANTITY_SRC', 'QUANTITY_TGT']].apply(lambda x:
  'U' if (x[0]==x[1] and ( (x[2]!=x[3]) or (x[4]!=x[5]) or (x[6]!=x[7])or (x[8]!=x[9]) ))
          else 'N', axis=1)

#create seperate dataset for New records

updateany = cust_joined_df.isin(['U']).any().any()
insertany = cust_joined_df.isin(['I']).any().any()
#print(insertany)
#print(updateany)


if (updateany == False and insertany == False):
    print('No Changes found in and made for  SalesOrderItems table')
elif(updateany == True and insertany == False):
    # #Create seperate dataset for UpDated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['SalesOrderItemsID_SRC' , 'PRODUCTID_SRC', 'SalesOrderID_SRC' , 'GROSSAMOUNT_SRC', 'QUANTITY_SRC']]
    upd_df.rename(columns={'SalesOrderItemsID_SRC':'SalesOrderItemsID','PRODUCTID_SRC':'PRODUCTID','SalesOrderID_SRC':'SalesOrderID','GROSSAMOUNT_SRC':'GROSSAMOUNT', 'QUANTITY_SRC':'QUANTITY'},inplace=True)
    # #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('SalesOrderItems', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'PRODUCTID':row.PRODUCTID , 'SalesOrderID':row.SalesOrderID , 'GROSSAMOUNT':row.GROSSAMOUNT , 'QUANTITY':row.QUANTITY})\
    		.where (sqla.and_(datatable.c.SalesOrderItemsID==row.SalesOrderItemsID))
    session.execute(upd)
    session.commit()
elif(updateany == False and insertany == True):
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['SalesOrderItemsID_SRC' , 'PRODUCTID_SRC', 'SalesOrderID_SRC' , 'GROSSAMOUNT_SRC' , 'QUANTITY_SRC']]
    ins_upd.rename(columns={'SalesOrderItemsID_SRC':'SalesOrderItemsID','PRODUCTID_SRC':'PRODUCTID','SalesOrderID_SRC':'SalesOrderID','GROSSAMOUNT_SRC':'GROSSAMOUNT','QUANTITY_SRC':'QUANTITY'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('SalesOrderItems', con = engine, if_exists = 'append', index=False)
    
else:
    ins_rec=cust_joined_df[cust_joined_df['INS_FLAG']=='I']
    ins_upd=ins_rec[['SalesOrderItemsID_SRC' , 'PRODUCTID_SRC', 'SalesOrderID_SRC' , 'GROSSAMOUNT_SRC', 'QUANTITY_SRC']]
    ins_upd.rename(columns={'SalesOrderItemsID_SRC':'SalesOrderItemsID','PRODUCTID_SRC':'PRODUCTID','SalesOrderID_SRC':'SalesOrderID','GROSSAMOUNT_SRC':'GROSSAMOUNT','QUANTITY_SRC':'QUANTITY'},inplace=True)
    #Insert new records to target table:
    ins_upd.to_sql('SalesOrderItems', con = engine, if_exists = 'append', index=False)
    
        #Create seperate dataset for UpDated records
    upd_rec=cust_joined_df[cust_joined_df['UPD_FLAG']=='U']
    upd_df=upd_rec[['SalesOrderItemsID_SRC' , 'PRODUCTID_SRC', 'SalesOrderID_SRC' , 'GROSSAMOUNT_SRC', 'QUANTITY_SRC']]
    upd_df.rename(columns={'SalesOrderItemsID_SRC':'SalesOrderItemsID','PRODUCTID_SRC':'PRODUCTID','SalesOrderID_SRC':'SalesOrderID','GROSSAMOUNT_SRC':'GROSSAMOUNT','QUANTITY_SRC':'QUANTITY' },inplace=True)
    # print(upd_df.head(5))
    
        #update records in target table
    metadata = sqla.MetaData(bind=engine)
    datatable = sqla.Table('SalesOrderItems', metadata, autoload=True)
    #loop over the dataframe items to update values in target
    for ind, row in upd_df.iterrows():
     	upd=sqla.sql.update(datatable)\
    		.values({'PRODUCTID':row.PRODUCTID , 'SalesOrderID':row.SalesOrderID , 'GROSSAMOUNT':row.GROSSAMOUNT,'QUANTITY':row.QUANTITY})\
    		.where (sqla.and_(datatable.c.SalesOrderItemsID==row.SalesOrderItemsID))
    session.execute(upd)
    session.commit()