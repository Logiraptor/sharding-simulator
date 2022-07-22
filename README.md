In my experiments I investigated the idea of allowing tenants to be “resharded” by adding an int64 seed value that gets mixed in to the shuffle sharding hash along with tenant and zone ids.

I found the following algorithm to work best, but my code doesn’t perfectly replicate the sharding behavior or tenant size distribution.

Compute a score for each tenant for how much it is contributing to the unbalanced ingesters:
- Add 1 for every ingester above the 95th percentile of total series
- Minus 1 for every ingester below the 95th percentile of total series

This score is high for tenants that are on overloaded ingesters and low for tenants on underloaded ingesters.

Reshard the top tenants by the score computed above with the following  configurable contraints:
- Reshard at most N tenants from each ingester to avoid excessive churn
- Reshard at most M active series (e.g. 1 large tenant or many small tenants that add to the same active series)
