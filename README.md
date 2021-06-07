README FILE
#Price Elasticity
This file describes the workflow used to obtain price elasticities and implementing algorithms
## Workflow
###Data preparation
Run `Top_5000_seller_produce_pickle_group_by_pre_campaign.py`: 
* Retrieve info from redshift/forecast tables and generate top5k items with enough Price variation and similarity

* Date is before campaign 28 of April

Run `whole_cat_produce_pickle_group_by.ipynb`:

* Produce the entire catalogue sales vs price for the clustering algorithm.

####TODO
Merge the two file with an option that changes the selection in order to run twice the same code rather than mantaining two different ones.


