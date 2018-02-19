# Social-Graph
Create a social graph of users with email dump using page rank algorithm

Steps:
1.Parse and load valid email address
2Get unique emails based on message id
3.Aggregate email count from every contact to every other contact(based on "TO","CC" & "BCC").
4.Apply page rank algorithm on aggregated values.
5.Use Neo4j to maintain/retrieve relationship graphs.

Preprocess :
1.Ignore emails from outside the organization.
