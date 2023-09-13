# Apriori_spark

This project inspects how Market Basket Analysis performs on the MeDAL dataset, a medical abbreviation disambiguation dataset for nat- ural language understanding. We implemented the well known A priori algorithm to discover frequent itemsets and association rules from trans- actional data. The analysis requires some preliminary processing of data, so they will also be object of discussion. And, finally, we understand the results of the analysis by focusing on three metrics of Market Basket Analysis: support, confidence, and interest.

We implemented the algorithm on a sample equal to 0.001% of the whole dataset. If you want to replicate the analysis, we reduced such parameter to 0.0001% in the released .ipynb. This should help significantly in reducing the running time of the whole algorithm. 

Moreover, for the same purpose, in the published notebook we also reduced the size of each basket to 3 items maximum. The results in our report refer to the analysis performed considering as the maximum possible size equal to 4 items, to get more detailed results. 

One could tune such parameter, according to the aime of his own analysis. 
