# Data Engineering code challenge

This code challenge is designed to assess a candidate's ability to work with data and programming languages, in this case Python and SQL. The challenge involves Extract, Transform and Load (ETL) tasks, as well as data cleaning and modeling.

The candidate is expected to use their skills in Python and SQL to fulfill the requirements stated below. Sample data sets are made available in the data folder.
The goal of the challenge is to evaluate the candidate's ability to work with real-world data scenarios and demonstrate their skills in data engineering.

## Specification

The given task involves working with three related datasets, `deal_sample`, `deal_activities_sample` and `deal_updates_sample`, which are all CSV files. These datasets represent business transactions in a CRM system, with a deal being a single unit. The `deal_activities_sample` and `deal_updates_sample` datasets describe what has happened to each deal, including the work done to win it.

By stakeholder request these data sets should be made easier to analyze and query, under an assumption that there are inconsistencies in the CRM system. Thus the goal of the task is to create a cleaner, more consistent view of the business transactions in the CRM system, which can be stored in a relational database.
 
To accomplish this, the candidate is expected to perform data cleaning and transformation tasks, create a database schema, and load the cleaned data into the database. They will need to determine the accuracy of the "total activities" field, which is a count of activities done per case, using the `deal_activities_sample` dataset. Only completed/done activities and those that haven't been deleted should be counted.

The stakeholder is also interested in knowing the average amount of updates made to each deal. This should only include updates to stages, values, and status. Deals that have neither activities nor updates should be marked as inactive. Finally, the stakeholder wants to know the number of calls and emails made for deals that have been won.

As an optional request, the stakeholder has asked if it is possible to only include deals which either have no activities or activities done within the last two years.

The candidate should make sure that the data is easily queryable for the stakeholder, as well as for future API access.
## Implementation requirements
Fill out the `main.py`, `create_statements.sql` and `transformations.sql` files with:
- Write SQL `CREATE TABLE` statements for each of the data sets
- Load data sets in Python. 
	- Exemplify how they can be loaded into the corresponding tables in a database
- Write SQL table(s) or view(s) for the transformed data as specified
- Show how the specified transformations can be done, either in Python or SQL.

## Discussion points

- What conclusions can be drawn about the quality of the data sets?
- How could a pipeline be set up to regularly update the database from the CRM system?
- How can the data be structured to accommodate future requests on the same underlying datasets?
- Which other relevant information could be retrieved from these data sets, if any?

## Notes
- Expected time usage around 2-4 hours
- Ambiguous statements are up for interpretation. Any choice is acceptable as long as it is reasoned for
- Return the completed task either as a zip or a link to a public repo