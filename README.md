# Bookscape_Explorer
This web application will extract data from online book APIs, store this information in a SQL database, and enable data analysis using SQL queries

### Problem Statement:
The BookScape Explorer project aims to facilitate users in discovering and analyzing book data through a web application. The application will extract data from online book APIs, store this information in a SQL database, and enable data analysis using SQL queries. The project will provide insights into book trends, user preferences, and reviews, helping users make informed reading choices while also offering a platform for community engagement. This initiative targets avid readers, researchers, and book enthusiasts.

### Business Use Cases:
 1. Search Optimization: Filter books based on genre, author, or publication year.
 2. Trend Analysis: Identify trending genres or authors over time.
 3. Data Insights: Perform analysis on user reviews and ratings to identify popular books.
 4. Decision Support: Provide insights for libraries or bookstores to stock trending books.

## Approach:
### Data Extraction: 
 - Utilize the Google Books API to gather comprehensive data on a variety of books.
 - Extract information such as book titles, authors, publication dates, genres, descriptions, user reviews etc
 - Implement pagination to retrieve more than the default number of results (e.g., fetching up to 1000 book entries).

### Data Storage: 
 - Create a SQL database with well-designed schema (e.g., defining appropriate data types and primary keys).
 - Insert data systematically and verify for consistency post-load.
### Data Analysis:
#### Use SQL queries to answer questions
 1. Check Availability of eBooks vs Physical Books
 2. Find the Publisher with the Most Books Published
 3. Identify the Publisher with the Highest Average Rating
 4. Get the Top 5 Most Expensive Books by Retail Price
 5. Find Books Published After 2010 with at Least 500 Pages
 6. List Books with Discounts Greater than 20%
 7. Find the Average Page Count for eBooks vs Physical Books
 8. Find the Top 3 Authors with the Most Books
 9. List Publishers with More than 10 Books
 10. Find the Average Page Count for Each Category
 11. Retrieve Books with More than 3 Authors
 12. Books with Ratings Count Greater Than the Average
 13. Books with the Same Author Published in the Same Year
 14. Books with a Specific Keyword in the Title
 15. Year with the Highest Average Book Price
 16. Count Authors Who Published 3 Consecutive Years
 17. Write a SQL query to find authors who have published books in the same year but under different publishers. Return the authors, year, and the COUNT of books they published in that year.
 18. Create a query to find the average amount_retailPrice of eBooks and physical books. Return a single result set with columns for avg_ebook_price and avg_physical_price. Ensure to handle cases where either category may have no entries.
 19. Write a SQL query to identify books that have an averageRating that is more than two standard deviations away from the average rating of all books. Return the title, averageRating, and ratingsCount for these outliers.
 20. Create a SQL query that determines which publisher has the highest average rating among its books, but only for publishers that have published more than 10 books. Return the publisher, average_rating, and the number of books published.


### Streamlit Application:
 - Design a user-friendly interface with input fields (e.g., search boxes or filters).
 - Connect the Streamlit app with the SQL database for real-time query execution.
 - Display analysis results in the form of tables, charts, or dashboards within the app.
### Deployment:
Deploy the SQL database and Streamlit app on a suitable platform 

### Results: 
The expected results of the project include a fully functional Streamlit application that enables users to search for and analyze book data extracted from APIs, a well-structured SQL database that stores book information, and a set of SQL queries that facilitate data analysis. Ultimately, learners should achieve hands-on experience in data extraction, SQL database management, and building interactive applications. This project will enhance their skills in data handling and application development while providing insights into the world of literature.

