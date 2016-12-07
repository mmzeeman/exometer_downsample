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
