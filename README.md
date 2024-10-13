# colibri_interview

##Overview
This code is written in Python and uses the PySpark environment and libraries. To run, import into a Databricks workspace and use a basic serverless compute.

##depedencies
Write access to catalogues called bronze and silver
read access to a catalog called landing with a schema called landed_files and a volume called vol_calibri

Data is held in a volume with path landing/landed_files/vol_colibri/ . We will consider this our immmutable "landing" environment, in which we store data "as is" without manipulation or cleansing.
We are working on the assumption that new records are added to the exisiting files therein, as opposed to new files arriving each period. 

##caveats
This code is written in Python, and as such does not leverage the spark.sql functionality that Databricks presents as best practice.
Similarly, I have overwritten the tables in databricks each time the code loads. Ideally I would use more complex merge statements to incrementally load the data and capture changes to historical data. With Delta Lake tables, we do get time travel for each overwrite so there _is_ some history, but this is not the solution I would have buit  given more time. 

There was guidance to cleanse the data as it arrived, however I have put conditions in place to stop the load before bronze instead of manipulating the data. This is consitent with my undersstanding of a multi-hop architecture, where data quality increases as data moves down stream. 

I have
