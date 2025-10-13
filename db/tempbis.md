### 1. Monthly-Wise Invoice Sales



Calculate monthly-wise invoice sales and sort them in descending order.


```sql


SELECT 


    DATE_FORMAT(InvoiceDate, '%Y-%m') AS Month,


    SUM(Total) AS TotalSales


FROM


    invoice


GROUP BY DATE_FORMAT(InvoiceDate, '%Y-%m')


ORDER BY TotalSales DESC;


```





### 2. Employee and Manager Names


Fetch the names of all employees and their managers.


```sql


SELECT 


    e.FirstName AS EmployeeName,


    e.LastName AS EmployeeLastName,


    m.FirstName AS ManagerName,


    m.LastName AS ManagerLastName


FROM


    employee e


        LEFT JOIN


    employee m ON e.ReportsTo = m.EmployeeId;


```





### 3. Customers Who Made Purchases in the USA


Find the names of customers who have made a purchase in the USA.


```sql


SELECT 


    FirstName, LastName


FROM


    customer


WHERE


    Country = 'USA';


```





### 4. Genre and Total Number of Tracks


Show the name of each genre and the total number of tracks in that genre.


```sql


SELECT 


    g.Name AS GenreName, COUNT(t.TrackId) AS TotalTracks


FROM


    genre g


        JOIN


    track t ON g.GenreId = t.GenreId


GROUP BY g.Name;


```





### 5. Customer Total Spending


Show the name of each customer and the total amount they have spent on purchases.


```sql


SELECT 


    c.FirstName, c.LastName, SUM(i.Total) AS TotalSpent


FROM


    customer c


        JOIN


    invoice i ON c.CustomerId = i.CustomerId


GROUP BY c.CustomerId , c.FirstName , c.LastName;


```





### 6. Album with the Highest Unit Price


Find the name of the album with the highest unit price.


```sql


SELECT 


    a.Title AS AlbumName


FROM


    album a


        JOIN


    track t ON a.AlbumId = t.AlbumId


ORDER BY t.UnitPrice DESC


LIMIT 1;


```





### 7. Missing Values Percentage


Display the percentage of missing values for `BillingState` and `BillingPostalCode` columns in the `invoice` table.


```sql


SELECT 


    (SUM(BillingState IS NULL) / COUNT(*)) * 100 AS BillingStateMissingPercentage,


    (SUM(BillingPostalCode IS NULL) / COUNT(*)) * 100 AS BillingPostalCodeMissingPercentage


FROM


    invoice;


```





### 8. Track Purchase Count


Show the name of each track and the total number of times it has been purchased.


```sql


SELECT 


    t.Name AS TrackName,


    COUNT(il.InvoiceLineId) AS TimesPurchased


FROM


    track t


        JOIN


    invoiceline il ON t.TrackId = il.TrackId


GROUP BY t.TrackId , t.Name;


```





### 9. Customer with the Largest Purchase


Find the name of the customer who has made the largest purchase in terms of total cost.


```sql


SELECT 


    c.FirstName, c.LastName, SUM(i.Total) AS TotalSpent


FROM


    customer c


        JOIN


    invoice i ON c.CustomerId = i.CustomerId


GROUP BY c.CustomerId , c.FirstName , c.LastName


ORDER BY TotalSpent DESC


LIMIT 1;


```





### 10. Customer Invoices and Spending


Display the total amount spent by each customer and the number of invoices they have.


```sql


SELECT 


    c.FirstName,


    c.LastName,


    COUNT(i.InvoiceId) AS NumberOfInvoices,


    SUM(i.Total) AS TotalSpent


FROM


    customer c


        JOIN


    invoice i ON c.CustomerId = i.CustomerId


GROUP BY c.CustomerId , c.FirstName , c.LastName;


```





### 11. Artist with Most Tracks


Find the name of the artist who has the most tracks in the Chinook database.


```sql


SELECT 


    a.Name AS ArtistName, COUNT(t.TrackId) AS TotalTracks


FROM


    artist a


        JOIN


    album al ON a.ArtistId = al.ArtistId


        JOIN


    track t ON al.AlbumId = t.AlbumId


GROUP BY a.ArtistId , a.Name


ORDER BY TotalTracks DESC


LIMIT 1;


```





### 12. Customers Spending Above Average


Find the names and email addresses of customers who have spent more than the average amount on invoices.


```sql


WITH AvgSpent AS (


    SELECT AVG(Total) AS AverageSpent


    FROM invoice


)


SELECT 


    c.FirstName,


    c.LastName,


    c.Email


FROM 


    customer c


JOIN 


    invoice i


ON 


    c.CustomerId = i.CustomerId


GROUP BY 


    c.CustomerId, c.FirstName, c.LastName, c.Email


HAVING 


    SUM(i.Total) > (SELECT AverageSpent FROM AvgSpent);


```





### 13. Artists in the 'Rock' Genre


List the names of all the artists that have tracks in the 'Rock' genre.


```sql


SELECT DISTINCT


    a.Name AS ArtistName


FROM


    artist a


        JOIN


    album al ON a.ArtistId = al.ArtistId


        JOIN


    track t ON al.AlbumId = t.AlbumId


        JOIN


    genre g ON t.GenreId = g.GenreId
WHERE
    g.Name = 'Rock';
```