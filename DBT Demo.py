# Databricks notebook source
# MAGIC %md
# MAGIC # Marketing Attribution DBT Demo

# COMMAND ----------

# MAGIC %md 
# MAGIC In Digital Marketing we try to influence user behavior to attain a desired outcome, whether it is a straight purchase of a product or service (hard conversion) or downloading specific content, or obtaining contact information during the acquisiton process. In order to do that, we can target the customer from different angles or marketing channels, for example they may see advertisements when using social media (Facebook or Instagram), when searching for content (Search Engine Marketing) or when browsing their favorite websites.
# MAGIC 
# MAGIC The problem is that in order to obtain a conversion, sometimes several advertising events will be needed, oftentimes from different marketing channels. Therefore, in order to properly allocate budget to marketing activities, an accurate view of the contribution of each channel to the target outcome is required, as ideally you want to tie optimization goals to revenue and ROI, rather that only taking cost into account. Marketing attribution is the process of assigning relative contributions to each channel. One player may score the goal, but the other team member providing an assist is also valuable to attain the desired result.
# MAGIC 
# MAGIC ![Digital Marketing Channels](https://digitaldiptesh.com/wp-content/uploads/2021/07/Make-Money-With-Digital-Marketing.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # The Dataset

# COMMAND ----------

# MAGIC %md We have 3 original tables, one for the user sessions, which links the source of the visit to the customer id, another table for the customers and the revenue they have generated and a third table that indicates how much money we have spent on every marketing channel on a daily basis.

# COMMAND ----------

spend = spark.read.format("csv").option("header","true").load("dbfs:/Users/carlos.delcacho@databricks.com/attribution/ad_spend.csv")
sessions = spark.read.format("csv").option("header","true").load("dbfs:/Users/carlos.delcacho@databricks.com/attribution/sessions.csv")
conversions = spark.read.format("csv").option("header","true").load("dbfs:/Users/carlos.delcacho@databricks.com/attribution/customer_conversions.csv")
spend.registerTempTable("spend")
sessions.registerTempTable("sessions")
conversions.registerTempTable("conversions")
display(spend)

# COMMAND ----------

# MAGIC %md
# MAGIC We can visualize the ad expenditures as time series with the native visualization capabilities of the notebooks in the Databricks platform, for example, we see in the following chart that the company spends more budget in paid social media advertisements than in search engine marketing:

# COMMAND ----------

display(spend)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have seen daily advertising costs, we can take a look at the revenue we are generating and compute the aggregated marketing ROI as revenue/cost:

# COMMAND ----------

display(conversions)

# COMMAND ----------

# MAGIC %md The ROI on all marketing activities, without taking into account personnel costs is:

# COMMAND ----------

revenue = spark.sql("select sum(revenue) as totalRevenue from conversions")
cost = spark.sql("select sum(spend) as totalCost from ad_spend");
print("The ROI is ",100*(revenue.first()[0]/cost.first()[0]-1),"%")

# COMMAND ----------

# MAGIC %md This company seems to be losing money at first. But bear in mind that initially acquired customers might become profitable in the long run due to repeat purchases, therefore the answer is more nuanced because the customers have not already churned. A more interesting question would be how much revenue is each marketing channel currently generating? In order to do this, we are going to have to join the session data with the customer data and try to attribute the revenue based on the marketing channel that the customer was acquired. For the ETL process we are going to use DBT:

# COMMAND ----------

# MAGIC %md
# MAGIC ![DBT Logo](https://miro.medium.com/max/1016/1*7HGfANnloBggMCl0u4RcYA.png)

# COMMAND ----------

# MAGIC %md
# MAGIC What is DBT?
# MAGIC DBT (Data Building Tool) is a command-line tool that enables data analysts and engineers to transform data in their warehouses simply by writing select statements.
# MAGIC DBT performs the T (Transform) of ETL but it doesn’t offer support for Extraction and Load operations. It allows companies to write transformations as queries and orchestrate them in a more efficient way. There are currently more than 280 companies running DBT in production, including well known brands such as HubSpot, Mango and The Telegraph.

# COMMAND ----------

# MAGIC %md
# MAGIC In order to use Databricks with DBT the first thing we have to do is create a Databricks personal access token

# COMMAND ----------

# MAGIC %md
# MAGIC ![Personal Access Token](https://i.stack.imgur.com/aAo1O.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Then we are going to create a user profile in ~/.databrickscfg with the required credentials and the workspace we want to connect to. Please see instructions available here on how to properly create thix text file: https://docs.databricks.com/dev-tools/cli/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC To configure DBT access in addition to this you are going to need to point to an specific cluster. In particular, you will need the host (workspace URL) and the HTTP path properties that can be obtained in the Advanced Options > JDBC/ODBC properties from the cluster configuration page:
# MAGIC ![Cluster Properties](https://www.rudderstack.com/static/a0277234d4588f9facb25c9522d8ba40/7d769/delta-lake-9.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You will then need to install DBT itself, as well as the Databricks DBT plugin that is available through PiPY:
# MAGIC 
# MAGIC ```
# MAGIC pip install dbt-core dbt-databricks
# MAGIC ```

# COMMAND ----------

# MAGIC %md Configuration is done by running dbt init. Please answer the questions being asked in the console with the information you have gathered in the previous steps. Now you can clone the DBT example project that has been adapted to work with Databricks from https://github.com/delcacho/dbt-databricks-demo. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Revisiting Marketing Attribution

# COMMAND ----------

# MAGIC %md
# MAGIC ### DBT Project structure

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Main project configuration is the dbt_project.yml file, that contains versioning information, required dependencies, as well as some pointers to important DBT folders.
# MAGIC 
# MAGIC There are several folders that are important in each DBT project:
# MAGIC 
# MAGIC - models: contains the SQL files that are used to perform transformations. The output of running these queries will be materialized as views in the schema that you define when running dbt init.
# MAGIC - data: contains what in DBT jargon are called seed files. These may be tables that are statically loaded into your project and that can be referenced through SQL transformations in the model files. These seed files for the marketing attribution project are CSVs with our prokect data.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's load the session, ad spend and customer data by running the seed command from the root folder where you have cloned the Git repository:
# MAGIC ```
# MAGIC dbt seed
# MAGIC ```

# COMMAND ----------

# MAGIC %md 
# MAGIC These CSV files are loaded as tables with the same name as the CSV file and can be referenced from DBT queries like in the following example:
# MAGIC 
# MAGIC ```
# MAGIC select * from {{ ref('customer_conversions') }}
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC The attribution logic is going to be specified in SQL in the attribution_touches.sql in the models folder:
# MAGIC 
# MAGIC ```
# MAGIC sessions_before_conversion as (
# MAGIC 
# MAGIC     select
# MAGIC         *,
# MAGIC 
# MAGIC         count(*) over (
# MAGIC             partition by customer_id
# MAGIC         ) as total_sessions,
# MAGIC 
# MAGIC         row_number() over (
# MAGIC             partition by customer_id
# MAGIC             order by sessions.started_at
# MAGIC         ) as session_index
# MAGIC 
# MAGIC     from sessions
# MAGIC 
# MAGIC     left join customer_conversions using (customer_id)
# MAGIC 
# MAGIC     where sessions.started_at <= customer_conversions.converted_at
# MAGIC         and sessions.started_at >= add_months(customer_conversions.converted_at,-30)
# MAGIC 
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md We are going to join the different tables together and count a conversion as being generated by an original session if the time spent between the customer first visited the site through a marketing channel and the actual conversion is less than 30 days. The effect of the advertisement is expected to be faded by then and if a purchase comes later, we don't interpret the original touchpoint as causing the registered conversion.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Attribution models](https://storage.googleapis.com/twg-content/images/Attribution_Model_Inline.width-1600.png)

# COMMAND ----------

# MAGIC %md
# MAGIC We are going to perform four simple attribution models:
# MAGIC - First touch attribution: All the credit for the conversion is given to the original session
# MAGIC - Last touch attribution: All the credit for the conversion is given to the last session before the conversion.
# MAGIC - Linear attribution: Equal weight among all sessions.
# MAGIC - Weight based attribution: 40%/20%/40%. More weight is given to the initial and final campaigns when compared with linear attribution

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The logic of these models can be found inside the file attribution_touches.sql in the models folder, Please look at the with_points query for details. In order to run the project, we run the following commands:
# MAGIC 
# MAGIC ```
# MAGIC dbt seed
# MAGIC dbt run
# MAGIC ```
# MAGIC 
# MAGIC This will take a while. It is going to generate a target folder with intermediate results as well as connect to the remote Databricks cluster to run the ETL queries, that will be run through the Spark execution engine in the workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC After running the commands, we can inspect the results of the ETL jobs that are materialized as views:

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW views IN attribution;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We could also have materialized the views into actual Delta tables by using the following statement inside our SQL file:
# MAGIC   
# MAGIC ```
# MAGIC {{ config(
# MAGIC   materialized='table',
# MAGIC   file_format='delta'
# MAGIC ) }}
# MAGIC ```

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from attribution.attribution_touches

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The most interesting columns of the result are the different revenue that we get for each session according to each simple attribution model. Now we can compute revenue by Marketing channel, for example for the last click attribution model. The company seems to be generating a large chunk of its revenue from Direct queries, that are normally attributed to SEO efforts:

# COMMAND ----------

# MAGIC %sql
# MAGIC select utm_source, sum(last_touch_revenue) as revenue from attribution.attribution_touches group by utm_source

# COMMAND ----------

# MAGIC %md
# MAGIC What is the revenue generated by Google search engine marketing according to each attribution model?

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(last_touch_revenue) as last_touch_revenue, sum(first_touch_revenue) as first_touch_revenue, sum(linear_revenue) as linear_revenue, sum(forty_twenty_forty_revenue) as forty_twenty_forty_revenue from attribution.attribution_touches where utm_source='google'

# COMMAND ----------

# MAGIC %md According to the linear revenue model, we should spend less in Google as attributed by the simple strategy of giving all weight to the last channel, as there appear to be more conversions where Google is the last click and other campaigns also took place to influence the buying decision.
