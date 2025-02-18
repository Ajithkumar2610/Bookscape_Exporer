import streamlit as st
import mysql.connector
import pandas as pd 
from mysql.connector import Error 
import requests
import time

def create_connction():
  try:
    conn = mysql.connector.connect(
      host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
      port = 4000,
      user = "4WJrof32emEzBny.root",
      password = "hbca9yBLx8nWUvwl",
      autocommit=True

    )
  
    conn.is_connected()
    st.success("Connected to MySQL database")
    return conn

  except Error as e:
    st.error(e)
    return None


mydb = create_connction()


api_key = 'AIzaSyAKOWSLpKTZde-Yd5VMSCnoFl6dWX0LjD8'

def scrap(query, api_key, max_results):
    url = "https://www.googleapis.com/books/v1/volumes"
    all_books = []  # To store all the book data
    seen_book_ids = set() #checking duplicate book id
    batch_size = 40  # Maximum number of results per request
    start_index = 0  # Starting index for pagination

    while len(all_books) < max_results:
        params = {
            "q": query,
            "key": api_key,
            "maxResults": min(batch_size, max_results - len(all_books)),
            "startIndex": start_index
        }
        response = requests.get(url, params=params)
        if response.status_code ==429:
            st.warning("Api limit reached , Retrying after 5 seconds..")
            time.sleep(5)
            continue

        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
            break

        data = response.json()
        items = data.get('items', [])
        if not items:  # Break if no more results
            break

        for item in items:
            if item['id'] not in seen_book_ids:
                seen_book_ids.add(item['id'])
                book_info = {
                    'Book_id' : item['id'],
                    'Search_key' : item.get('searchInfo',{}).get('textSnippet','NA'),
                    'Book_title' : item['volumeInfo'].get('title','NA'),
                    'Book_subtitle' : item['volumeInfo'].get('subtitle','NA'),
                    'Book_authors' : item['volumeInfo'].get('authors','NA'),
                    'Book_description' : item['volumeInfo'].get('description','NA'),
                    'publisher' : item['volumeInfo'].get('publisher','NA'),
                    'IndustryIdentifiers' : item['volumeInfo'].get('industryIdentifiers','NA'),
                    'Text_readingmodes' : item['volumeInfo'].get('readingModes',{}).get('text','NA'),
                    'Image_readingmodes' : item['volumeInfo'].get('readingModes',{}).get('image','NA'),
                    'Page_count' : item['volumeInfo'].get('pageCount','NA'),
                    'Categories' : item['volumeInfo'].get('categories','NA'),
                    'Language' : item['volumeInfo'].get('language','NA'),
                    'Image_links' : item['volumeInfo'].get('imageLinks',{}).get('thumbnail','NA'),
                    'Ratings_count' : item['volumeInfo'].get('ratingsCount','NA'),
                    'Average_Rating' : item['volumeInfo'].get('averageRating','NA'),
                    'Country' : item['accessInfo'].get('country','NA'),
                    'Saleability' : item['saleInfo'].get('saleability','NA'),
                    'isEbook' : item['saleInfo'].get('isEbook','NA'),
                    'Amount_listPrice' : item['saleInfo'].get('listPrice',{}).get('amount','NA'),
                    'CurrencyCode_listPrice' : item['saleInfo'].get('listPrice',{}).get('currencyCode','NA'),
                    'Amount_retailPrice' : item['saleInfo'].get('retailPrice',{}).get('amount','NA'),
                    'CurrencyCode_retailPrice' : item['saleInfo'].get('retailPrice',{}).get('currencyCode','NA'),
                    'Buy_link' : item['saleInfo'].get('buyLink','NA'),
                    'year' : item['volumeInfo'].get('publishedDate','NA'),
                }
                all_books.append(book_info)

        start_index += len(items)  # Increment start index for the next batch

    # Convert to DataFrame
    books_df = pd.DataFrame(all_books)
    return books_df

def save_database():
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)

            mycursor.execute("use book_list")

            df = pd.read_csv(r"C:\Guvi_project\Project_1\scraps_books_data.csv")
            st.success("data loaded from CSV successfully")
            
            mycursor.execute('drop table if exists book_list.bkdetails')
            
            mycursor.execute('''create table book_list.bkdetails(
            Book_id varchar(100) primary key,
            Search_key varchar(300), Book_title varchar(250),
            Book_subtitle text, Book_authors text,
            Book_description text, publisher text, IndustryIdentifiers text,
            Text_readingmodes boolean, Image_readingmodes boolean,
            Page_count int, Categories text, Language varchar(50),
            Image_links text, Ratings_count int,
            Average_Rating decimal, Country varchar(50),
            Saleability varchar(250), isEbook boolean,
            Amount_listPrice decimal, Currencycode_listPrice varchar(50),
            Amount_retailPrice decimal, Currencycode_retailPrice varchar(50),
            Buy_Link text, year text )''')
            
            for index, row in df.iterrows():

                # Replace NaN with None
                row = row.where(pd.notnull(row), None)

                values = (row.Book_id, row.Search_key, row.Book_title, row.Book_subtitle, row.Book_authors, row.Book_description, row.publisher, row.IndustryIdentifiers,
                            row.Text_readingmodes, row.Image_readingmodes, row.Page_count, row.Categories, row.Language, row.Image_links, row.Ratings_count,
                            row.Average_Rating, row.Country, row.Saleability, row.isEbook, row.Amount_listPrice, row.CurrencyCode_listPrice, row.Amount_retailPrice,
                            row.CurrencyCode_retailPrice, row.Buy_link, row.year)


                mycursor.execute('''INSERT IGNORE INTO book_list.bkdetails(
                    Book_id,Search_key, Book_title, Book_subtitle, Book_authors, Book_description, publisher, IndustryIdentifiers,
                    Text_readingmodes, Image_readingmodes, Page_count, Categories, Language, Image_links, Ratings_count,
                    Average_Rating, Country, Saleability, isEbook, Amount_listPrice, CurrencyCode_listPrice, Amount_retailPrice,
                    CurrencyCode_retailPrice, Buy_link, year) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                    values)
                

                mydb.commit()
            st.success("All datas stored in Database")
        except Error as e:
            st.error(e)
        finally:
            mycursor.close()



def search_book():
        query = st.text_input("Enter the book name or category")
        max_results = 1200
        if st.checkbox("search book"):
            books_data = scrap(query, api_key, max_results)
            st.write(f"Total number of books found: {len(books_data)}")
            books_data.to_csv('scraps_books_data.csv', index=False)
            st.success("data stored in csv")
            if st.button("Store in database"):
                save_database()
            

def top_queries(action):
    #1. Availability of Ebook
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query1= "(select isEbook, count(*) from book_list.bkdetails group by isEbook)"

            #action = st.sidebar.selectbox("select an option",["","Availability of books"])

            if action =="1.Availability of books":
                mycursor.execute(query1)
                result = mycursor.fetchall()
                df1 = pd.DataFrame(result,columns=["Ebook","count"])
                st.markdown('''Ebook availabilty data: <br>
                            Row 1 is Ebook <br>
                            Row 0 is Physical book''',True)
                st.write("Ebook availabilty data")
                st.dataframe(df1,use_container_width=True)

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()


    #2. find the publisher with the most books published
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)

            qurey2 = ("""
            SELECT publisher, COUNT(*) AS book_count 
            FROM book_list.bkdetails
            WHERE publisher IS NOT NULL
            GROUP BY publisher 
            ORDER BY book_count DESC 
            
        """)
    
            

            if action ==  "2.Top Publisher":
                mycursor.execute(qurey2)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["publisher","book_count"])
                st.write("Top Publisher")
                st.dataframe(df,use_container_width=True)

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()
            

    #3. publisher with the highest average rating
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query3=("""
            SELECT publisher, AVG(Average_Rating) AS avg_rating 
            FROM book_list.bkdetails
            WHERE publisher IS NOT NULL and Average_Rating >=5.000
            GROUP BY publisher 
            ORDER BY avg_rating DESC 

    """)

            if action == "3.publisher with the highest average rating":
                mycursor.execute(query3)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["publisher","avg_rating"])
                st.write("publisher with the highest average rating")
                st.dataframe(df,use_container_width=True)
                

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()


    #4. Top 5 most expensive books by retail price
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query4=("""
            SELECT Book_title, Amount_retailPrice 
            FROM book_list.bkdetails
            WHERE Amount_retailPrice IS NOT NULL
            ORDER BY Amount_retailPrice DESC 
            LIMIT 5

    """)

            if action == "4.Top 5 most expensive books by retail price":
                mycursor.execute(query4)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["Book_title","Amount_retailPrice"])
                st.write("Top 5 most expensive books by retail price")
                st.dataframe(df,use_container_width=True)

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()

    #5. Books published after 2010 with at least 500 pages
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query5=("""
            SELECT Book_title, Page_count, year
            FROM book_list.bkdetails
            WHERE Page_count >= 500 AND year > 2010

    """)

            if action == "5.Books published after 2010 with at least 500 pages":
                mycursor.execute(query5)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["Book_title","Page_count","year"])
                st.write("Books published after 2010 with at least 500 pages")
                st.dataframe(df,use_container_width=True)
                

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()

    #6. lists book with discounts more than 20%
    if mydb:
        try:
            mycursor = mydb.cursor()
            query6=("""
            SELECT Book_title, Amount_retailPrice, Amount_listPrice, 
                    Round(((Amount_retailPrice - Amount_listPrice) / Amount_retailPrice)*100,2) AS discount
            FROM book_list.bkdetails
            WHERE Amount_retailPrice IS NOT NULL AND 
                Amount_listPrice IS NOT NULL AND 
                (Amount_retailPrice - Amount_listPrice) / Amount_retailPrice > 0.2

    """)

            if action == "6.lists book with discounts more than 20%":
                mycursor.execute(query6)
                result = mycursor.fetchall()
                #st.write(f"no of results: {len(result)}")

                if not result:
                    st.write("No books found with discounts more than 20%")
                else:
                    df = pd.DataFrame(result,columns=["Book Title","Amount_retailPrice","Amount_listPrice","discount"])
                    st.write("Books with discounts more than 20%")
                    st.dataframe(df,use_container_width=True)
                    
        except Error as e:
            st.error(e)
        finally:
            mycursor.close()


    # 7.find the average page count for ebooks vs physical books
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query7=("""
            SELECT 
            CASE
                WHEN isEbook = 1 THEN 'Ebook'
                ELSE 'Physical'
            END AS book_type,
            AVG(Page_count) AS avg_page_count
            FROM book_list.bkdetails
            GROUP BY book_type

    """)

            if action == "7.find the average page count for ebooks vs physical books":
                mycursor.execute(query7)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["Book_Type","Page_count"])
                st.write("Average page count for ebooks vs physical books")
                st.dataframe(df,use_container_width=True)
                

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()

    # 8.top 3 authors with the most books
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query8=("""
            SELECT Book_authors, COUNT(*) AS book_count 
            FROM book_list.bkdetails
            WHERE Book_authors IS NOT NULL
            GROUP BY Book_authors 
            ORDER BY book_count DESC 
            LIMIT 3

    """)

            if action == "8.top 3 authors with the most books":
                mycursor.execute(query8)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["Book_Authors","Book_count"])
                st.write("Top 3 authors with the most books")
                st.dataframe(df,use_container_width=True)
                

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()

    # 9.list publisher with more than 10 books
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query9=("""
            SELECT publisher, COUNT(*) AS book_count 
            FROM book_list.bkdetails
            WHERE publisher IS NOT NULL
            GROUP BY publisher 
            HAVING book_count > 10
            ORDER BY book_count DESC

    """)

            if action == "9.list publisher with more than 10 books":
                mycursor.execute(query9)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["Publisher","Book_count"])
                st.write("Publisher with more than 10 books")
                st.dataframe(df,use_container_width=True)
                

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()


    # 10.find the average page count of each category
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query10=("""
            SELECT Categories, AVG(Page_count) AS avg_page_count 
            FROM book_list.bkdetails
            GROUP BY Categories
            ORDER BY avg_page_count DESC

    """)

            if action == "10.find the average page count of each category":
                mycursor.execute(query10)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["categories","avg_page_count"])
                st.write("Each categories average page count")
                st.dataframe(df,use_container_width=True)
                

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()


    # 11. books with more than 3 authors
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query11 = """
            SELECT book_title, book_authors,
            LENGTH(book_authors) - LENGTH(REPLACE(book_authors, ',', ''))+1 As authors_count
            FROM book_list.bkdetails
            WHERE LENGTH(book_authors) - LENGTH(REPLACE(book_authors, ',', '')) >= 3
            """
            
            if action == "11.books with more than 3 authors":
                mycursor.execute(query11)
                result = mycursor.fetchall()
                
                if result:
                    df = pd.DataFrame(result, columns=["Book_Title", "Book_Authors","Authors_count"])
                    st.write("Books with more than 3 authors")
                    st.dataframe(df,use_container_width=True)
                else:
                    st.write("No books with more than 3 authors found.")
                    
        except Error as e:
            st.error(e)
        finally:
            mycursor.close()

    #12.books with rating count greter than the average
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query12=("""
                    SELECT Book_title, Ratings_count 
                    FROM book_list.bkdetails
                    WHERE Ratings_count > (SELECT AVG(Ratings_count) FROM book_list.bkdetails)

    """)

            if action == "12.books with rating count greter than the average":
                mycursor.execute(query12)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["Book_Title","Ratings_count"])
                st.write("Books with rating count greter than the average")
                st.dataframe(df,use_container_width=True)
                

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()



    #13. books with same author published in the same year
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query13=("""
                    SELECT year(year) as year , GROUP_CONCAT(book_title) , book_authors
                    FROM book_list.bkdetails
                    where book_authors is not null and year is not null
                    group by book_authors, year
                    having count(*) > 1

    """)

            if action == "13.books with same author published in the same year":
                mycursor.execute(query13)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["Year","Books","Authors"])
                st.write("Books with same author published in the same year")
                st.dataframe(df,use_container_width=True)
                

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()

    #14. books with a specific keyword in the title
    if mydb:
        try:
            if action == "14.books with a specific keyword in the title":
                keyword = st.text_input("Enter a keyword to search a books:")
                if keyword:
                    mycursor = mydb.cursor(buffered=True)

                    query14=("""
                    SELECT Book_title
                    FROM book_list.bkdetails
                    WHERE Book_title LIKE %s

            """)

                    
                    mycursor.execute(query14,(f"%{keyword}%",))
                    result = mycursor.fetchall()

                    if result:
                        df = pd.DataFrame(result,columns=["Book_Title"])
                        st.write("Books with a specific keyword in the title")
                        st.dataframe(df,use_container_width=True)
                    else:
                        st.write(f"No book found with the keyword '{keyword}' in this title")
                    

        except Error as e:
            st.error(e)
        finally:
            if mycursor in locals():
                mycursor.close()

    #15. year with the highest average book price
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query15=("""
                    SELECT YEAR(year), AVG(Amount_retailPrice) AS avg_price
                    FROM book_list.bkdetails
                    WHERE year IS NOT NULL AND Amount_retailPrice IS NOT NULL
                    GROUP BY year
                    ORDER BY avg_price DESC
                    LIMIT 1

    """)

            if action == "15.year with the highest average book price":
                mycursor.execute(query15)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["Year","Avg_price"])
                st.write("year with the highest average book price")
                st.dataframe(df,use_container_width=True)
                

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()


    #16. count authors who published consecutive 3 years
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query16=("""
                    SELECT Book_authors, CAST(year As UNSIGNED) as publication_year,count(*) as book_count
                    FROM book_list.bkdetails
                    WHERE year IS NOT NULL and Book_authors IS NOT NULL
                    GROUP BY Book_authors ,publication_year
                    HAVING count(*) >= 3

    """)

            if action == "16.count authors who published consecutive 3 years":
                mycursor.execute(query16)
                result = mycursor.fetchall()
                if result:
                    df = pd.DataFrame(result,columns=["Book_Author","publication_year","consecutive_book_count"])
                    st.write("count authors who published consecutive 3 years")
                    st.dataframe(df,use_container_width=True)
                else:
                    st.write("Anybody not published consecutive 3 years")

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()



    #17. find author who published the book same year under different publisher
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query17=("""
                    SELECT book_authors, YEAR(year), COUNT(*) AS book_count
                    FROM book_list.bkdetails
                    WHERE year IS NOT NULL AND book_authors IS NOT NULL AND publisher IS NOT NULL
                    GROUP BY book_authors, year
                    HAVING COUNT(DISTINCT publisher) > 1;

    """)

            if action == "17.find author who published the book same year under different publisher":
                mycursor.execute(query17)
                result = mycursor.fetchall()
                if result:
                    df = pd.DataFrame(result,columns=["Book_Author","Year","Book_count"])
                    st.write("Find author who published the book same year under different publisher")
                    st.dataframe(df,use_container_width=True)
                else:
                    st.write("Anybody not published the book same year under different publisher")

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()


    #18.average amount_retailPrice of eBooks and physical books
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query18=("""
                    SELECT
                    AVG(CASE WHEN isEbook = TRUE THEN Amount_retailPrice ELSE NULL END) AS avg_ebook_price,
                    AVG(CASE WHEN isEbook = FALSE THEN Amount_retailPrice ELSE NULL END) AS avg_physical_price
                    FROM book_list.bkdetails;

    """)

            if action == "18.average amount_retailPrice of eBooks and physical books":
                mycursor.execute(query18)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["Avg_Ebook_price","Avg_Physicalbook_price"])
                st.write("Average amount_retailPrice of eBooks and physical books")
                st.dataframe(df,use_container_width=True)
                

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()



    #19.books that have an averageRating that is more than two standard deviations away from the average rating of all books
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query19=("""
            SELECT book_title, Average_Rating, ratings_Count
            FROM book_list.bkdetails
            WHERE Average_Rating > (SELECT AVG(Average_Rating) + 2 * STDDEV(Average_Rating) FROM book_list.bkdetails)
            OR Average_Rating < (SELECT AVG(Average_Rating) - 2 * STDDEV(Average_Rating) FROM book_list.bkdetails);

    """)

            if action == "19.books that have an averageRating that is more than two standard deviations away from the average rating of all books":
                mycursor.execute(query19)
                result = mycursor.fetchall()
                df = pd.DataFrame(result,columns=["Book_Title","Average_Rating","Rating_count"])
                st.write("""Books that have an averageRating that is more than two standard deviations
                        away from the average rating of all books""")
                st.dataframe(df,use_container_width=True)
                

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()

    #20.which publisher has the highest average rating among its books, but only for publishers that have published more than 10 books
    if mydb:
        try:
            mycursor = mydb.cursor(buffered=True)
            query20=("""
                SELECT publisher, AVG(average_rating) AS average_rating, COUNT(*) AS book_count
                FROM book_list.bkdetails
                WHERE publisher IS NOT NULL
                GROUP BY publisher
                HAVING COUNT(*) > 10
                ORDER BY average_rating DESC
                LIMIT 1;

    """)

            if action == "20.who published more than 10 books has the highest average rating among its books":
                mycursor.execute(query20)
                result = mycursor.fetchall()
                if result:
                    df = pd.DataFrame(result,columns=["Publisher","Average_Rating","Book_count"])
                    st.write("""Which publisher has the highest average rating among its books,
                            but only for publishers that have published more than 10 books """)
                    st.dataframe(df,use_container_width=True)
                else:
                    st.write("No publisher found with  more than 10 Books")

        except Error as e:
            st.error(e)
        finally:
            mycursor.close()

pages = st.sidebar.selectbox("Books scrap",["search book","Top Queries"])

if pages == "search book":
    search_book()
else:
    pass



if pages == "Top Queries":
    queries = st.selectbox("Top Querys",["Select One","1.Availability of books","2.Top Publisher",
                                    "3.publisher with the highest average rating",
                                    "4.Top 5 most expensive books by retail price",
                                    "5.Books published after 2010 with at least 500 pages",
                                    "6.lists book with discounts more than 20%",
                                    "7.find the average page count for ebooks vs physical books",
                                    "8.top 3 authors with the most books",
                                    "9.list publisher with more than 10 books",
                                    "10.find the average page count of each category",
                                    "11.books with more than 3 authors",
                                    "12.books with rating count greter than the average",
                                    "13.books with same author published in the same year",
                                    "14.books with a specific keyword in the title",
                                    "15.year with the highest average book price",
                                    "16.count authors who published consecutive 3 years",
                                    "17.find author who published the book same year under different publisher",
                                    "18.average amount_retailPrice of eBooks and physical books",
                                    "19.books that have an averageRating that is more than two standard deviations away from the average rating of all books",
                                    "20.who published more than 10 books has the highest average rating among its books"])

    top_queries(queries)
else:    
    pass


