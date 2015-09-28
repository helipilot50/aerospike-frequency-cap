#Frequency capping using Key-value operations

##Problem

I want to count the number of times a user has been presented a campaign for frequency capping. How do I use Key-value operations to do this?

##Solution
The simple solution is to keep a count of the presentations, per user, per day. Before presenting a campaign, read the past 10 days history for that campaign only and total the daily count.


##Discussion

The secret is designing a composite key to match how you will read the data. In this case we want to know the count by user, campaign and over the past 10 days.

The key consists of:
- UserID
- CampaignID
- Date - this is only a date, not a time stamp

The value is a counter of presentations for that day.

To het the number of times a campaign has been presented in the last 10 days, formulate 10 keys (as described above) and batch read all 10 records. 

The result will be aan array of 10, or less, records. Iterate over the array and for each element that is not null, add the count to the total for the 10 days.


