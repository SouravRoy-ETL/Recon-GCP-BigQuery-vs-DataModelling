# Recon-GCP-BQ-vs-DataModelling
> Creating automated Reconcilliation script to check Integrity of the GCP BigQuery DWH with Data Modelling sheet created by client Data Modelling team. Also, to check the DDL statements of Oracle DB (Source) vs GCP BigQuery.
This activity has reduced resource focusing on Manual Tasks by 99%. Fault tolerance is 1% in case of wrong input passed by triggers. 

# Results:
1) GCP BigQuery vs Data Modelling
![image](https://user-images.githubusercontent.com/85476817/122936035-78c62380-d38e-11eb-8177-816786eae0ef.png)
2) DDL vs GCP BigQuery
![image](https://user-images.githubusercontent.com/85476817/122936490-d2c6e900-d38e-11eb-99a6-3e676644b54f.png)

# Installing
This Framework was created on **Python 3.8.5** and uses some external libraries listed below:

### b) Pandas
### c) Numpy

# Build/Run Command
Use following commands to build/Run the project from the project root. 
This script accepts 3 inputs and generates 2 YML Files
### Mapping Sheet (Excel File which has the Table_Name and Columns in rows)
### Sheet Name of the Above Excel sheet
### Config File which contains (bq.Table, bq.dataset, etc)
````
python .\Run_YML_Creater.py "Mapping_Sheet" "Excel_sheet_name" "Config_file_name"
````

### Authors
* Sourav Roy (souravroy7864@gmail.com)
