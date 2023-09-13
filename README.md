# Apriori_spark

This project inspects how Market Basket Analysis performs on the MeDAL dataset, a medical abbreviation disambiguation dataset for nat- ural language understanding. We implemented the well known A priori algorithm to discover frequent itemsets and association rules from trans- actional data. The analysis requires some preliminary processing of data, so they will also be object of discussion. And, finally, we understand the results of the analysis by focusing on three metrics of Market Basket Analysis: support, confidence, and interest. We implemented the algorithm on a sample equal to 0.001% of the whole dataset and 4 items per basket. 

In the published notebook we reduced the size of each basket to 3 items and a considered the 0.1 of the data, for the sake of faster computational time. 
One could tune such parameter, according to the aim of his/her own analysis. 
