Ibotta Analytics Engineering Take Home Project
=========

We’ve found that one of the best ways to evaluate engineering candidates is a take-home project. This project gives you a chance to work on some relevant code at home, and it gives us something concrete to discuss during your in-person interviews.

## Part 1 ## - Python code development and comprehension
##########################

You have been given 4 csv files of data and 1 empty sqlite database (ibotta.db)
Using PYTHON, write some code to load the 4 csv files into a database with their respective table names (offer_rewards, customer_offers, customer_offer_rewards, customer_offer_redemptions). As mentioned, there is provided an empty sqlite db for your convenience but you can use your own type of SQL database if you so choose.

Note: Provided is some fair amount of Python code of which you can use to build upon. Please provide comments to the code to gauge your Python reading comprehension. You can opt to write your own Python code (or modify/improve the existing), but again, provide comments, consider OOP design, code quality, and flexibility/extensibility.

SUBMISSION:
Provide your Python scripts and database with the populated tables.


## Part 2 ## - SQL Queries, Fact Tables, and data analysis
#########################

Customers use Ibotta to look for ways to save/get money on items they look to purchase by finding deals/offers. When a customer sees an offer they like, they add it to their account. The offer is honored once the customer makes the required purchase(s) and is verified with Ibotta.
(ex. customer adds the '$3.00 back when you buy cheese' offer to their account. Upon actual purchase and verification, $3.00 is then credited back to the customer)
As a note, these offers can possibly be redeemed multiple times. (There is a 1 to many relation between offers and redemptions)

offer_rewards - Offers available to customers
customer_offers - Offers selected by the customer (offer is activated) and timestamped when verified completed
customer_offer_rewards - Link table between selected customer offers and rewards
customer_offer_redemptions - redemption information (ex. how many times redeemed, max times that can be redeemed, etc.)

Given the populated tables of Part 1, use SQL queries to provide answers to the following questions:

1) What is the total counts of offer activations for each customer?

2) Provide a list of customers that who haven't activated an offer in the last couple months?

3) What is the conversion rate of activated to complete for each customer?

4) What is the total amount of redemption for each customer?

SUBMISSION:
Please provide all your SQL queries and created table(s)
Note: Describe your thought process on the queries


# Deliverable
Please provide the code for the assignment with execution instructions either in a private repository (GitHub or Bitbucket) or as a zip file.
