# exometer_sqlite

WIP reporter for exometer. 

## Storage Model

The statistics are stored in separate tables. The tables will hold samples about 600 samples each for each period.

The periods are:

 * hour
 * day
 * week
 * month
 * 3 months
 * 6 months
 * year

The samples will flow from the short period table to the longer period table. This means the resolution of the 
samples will decrease over time.

## Subscription

When the reporter is subscribed to a metric the tables will be automatically created and the data will be stored
in the tables. 

When your metric is named [metric, test1] a table named "[metric,test1,hour]" stores the hour samples.
