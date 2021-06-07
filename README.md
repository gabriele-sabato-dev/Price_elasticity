README FILE
#Price Elasticity
This file describes the workflow used to obtain price elasticities and implementing algorithms
## Workflow

###Data preparation
1) Update all the tables, also the similarity score one :) 
2) Run `Top_5000_seller_produce_pickle_group_by_pre_campaign.py`: 
    * Retrieve info from redshift/forecast tables and generate top5k items with enough Price variation and similarity

    * Date is before campaign 28 of April


3) Run `whole_cat_produce_pickle_group_by.ipynb`:

    * Produce the entire catalogue sales vs price for the clustering algorithm. 

4) Run `Model_sales_pre_campaign.ipynb`:
   * Run the Price elasticity model based on sales vs log(price) + del_week + intercept
####TODO
Merge the two file with an option that changes the selection in order to run twice the same code rather than mantaining two different ones.


