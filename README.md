# vehicle-sales-analysis
Utilizing international business-to-business order sales for almost 2.5 years:
1) Created a relational database with 15 tables using PostgreSQL
2) Built a behavioral customer segmentation model using RFM analysis and machine learning to group customers into three distinct segments
3) Loaded segmentation results into the database for further analysis

## Customer Segmentation Model
Utilizes RFM (Recency, Frequency, Monetary) analysis to segment customers based on purchasing behavior.

### RFM Metrics
- **Recency:** number of days since last order
- **Frequency:** number of orders made during analysis period
- **Monetary:** amount of money spent during analysis period

### Model Selection
Evaluated 30+ clustering model combinations using K-Means and K-Medoids (PAM) algorithms--after preprocessing the data. The final model uses **K-Medoids/PAM** clustering:
- Better handling of outliers compared to K-Means
- Scored well in evaluation metrics (Silhouette score, DB Index, and CH Index)
- Significantly faster runtime than K-Means

### Results
The model identifies three distinct customer segments. Here's a quick overview:
![image](https://github.com/user-attachments/assets/c7678388-64e4-456f-bb91-ef5540143dc3)


## Relational Database
Uses PostgreSQL with both SQL scripts and Psycopg2 (Python adapter) to manage a relational database with 15 tables.

### Core Data
- **Order Management:** stores customers, addresses, orders, and product information
- **Customer Segmentation:** maintains RFM analysis results including segment labels

### Data Quality & Validation
- **Address Validation:** geocodes and standardizes addresses using [ArcGIS REST API's Geocoding service](https://developers.arcgis.com/rest/geocode/geocode-addresses/)
- **Phone Number Validation:** formats and validates phone numbers using the [phonenumbers module](https://pypi.org/project/phonenumbers/)

### Data Management
- **Audit Trail:** tracks all insertions, updates, and deletions with user attribution and timestamps
- **Incremental Loading:** uses matermarks to enable efficient incremental data processing
- **Version Control:** maintains historical snapshots of customer segmentation models with analysis timeframes

## Repo Layout
- **analysis**
    - rfm_modeling.ipynb--> preprocesses the data from the database then finds the best customer segmentation model
    - segment_analysis.ipynb--> analyzes the RFM values from the final customer segmentation model
- **config**/rfm_dates.json--> dates used in RFM analysis
- **data**
    - **derived**/rfm_labels.csv--> RFM analysis and customer segmentation model results
    - **raw**/sales_data_sample.csv--> raw order data
- **db**--> SQL scripts to be applied in elt folder
    - 01_schema.sql
    - 02_refresh_core.sql
    - 03_load_rfm.sql
- **elt**--> Python scripts to create/interact with the database using primarily Psycopg2 and SQL scripts from db folder
    - 01_bootstrap_db.py
    - 02_dowload_raw.py
    - 03_load_raw.py
    - 04_transform_raw.py
    - 05_load_rfm.py
- **.env**--> contains access_token to use ArcGIS REST API, and postgres credentials (maintenance_db, super_user, pg_password, host, port)

## Data Source
**Sales Data:** [Retail Dataset from Kaggle](https://www.kaggle.com/datasets/kyanyoga/sample-sales-data)  

## References/Acknowledgments
- `silhouette_diagnostic()` function implementation adapted from [scikit-learn documentation](https://scikit-learn.org/stable/auto_examples/cluster/plot_kmeans_silhouette_analysis.html)
- Address validation via [ArcGIS REST API](https://developers.arcgis.com/rest/geocode/)
- Phone number validation via [phonenumbers library](https://pypi.org/project/phonenumbers/)
