README FILE
#Price Elasticity
This file describes the workflow used to obtain price elasticities and implementing algorithms
## Workflow

###Data preparation
1) Update all the tables, also the similarity score one :) 
   * Introduce Top10k and introduce voucher value
2) Run `Top_5000_seller_produce_pickle_group_by_pre_campaign.py`: 
    * Retrieve info from redshift/forecast tables and generate top5k items with enough Price variation and similarity


    * Date is before campaign 28 of April
   
3) Run `whole_cat_produce_pickle_group_by.ipynb`:

    * Produce the entire catalogue sales vs price for the clustering algorithm. 

###Model implementation
1) Run `Model_sales_pre_campaign.ipynb`:
   * Run the Price elasticity model based on sales vs log(price) + del_week + intercept

###Margin optimization   
1) Run `Sales_Margin_also.ipynb`
   * Run the algorithm that optimizes the margin for progressive in(de)crement in price until 14%.

###Prepare file for Captains
1) Run `Create_captains_ready_file.ipynb`
   * Inner join from 
### HomieDaysTest   
1) Run `Margin_Homie14.ipynb`
   * Filter for the items in HomieDays bazooka.
   * Almost majority of items does have a campaign before even the beginning of homie days.
2) Run `Margin_calculation_pre_post_Homiedays_campaign.ipynb`
    * Use N_Sales * (Price - PC2) 
   TODO Fix the query
   
   
1) Enjoy life, keep surfing
####TODO
Merge the two file with an option that changes the selection in order to run twice the same code rather than mantaining two different ones.


