--top 10 venues by capacity --
SELECT top 10 Venue, Capacity
FROM baksteballVenues
ORDER BY Capacity DESC;

-- average capacity by country --
SELECT country, AVG(Capacity) as avg_capacity
FROM baksteballVenues
GROUP BY country
ORDER BY avg_capacity DESC;

-- teams with the largest venues --
SELECT Basketballteam, Venue, Capacity 
FROM baksteballVenues 
ORDER BY Capacity DESC;

--count of venues in each country--
SELECT country, count(country) stadium_count
FROM baksteballVenues
GROUP BY country
ORDER BY stadium_count desc, country asc

--venue ranking within each country--
SELECT venue, Country,
    RANK() OVER(PARTITION BY country ORDER BY capacity DESC) as country_rank
FROM baksteballVenues;

--top 3 venues ranking within each country--
SELECT Venue, Country, capacity, country_rank
FROM (
    SELECT Venue, Country, capacity,
           RANK() OVER (PARTITION BY country ORDER BY capacity DESC) as Country_rank
    FROM baksteballVenues
) ranked_venues
WHERE Country_rank <= 3;

-- grouping venues by capacity range --
SELECT 
    CASE 
        WHEN Capacity < 15000 THEN 'Small'
        WHEN Capacity BETWEEN 15000 AND 20000 THEN 'Medium'
        ELSE 'Large'
    END AS CapacityRange,
    COUNT(Venue) AS VenueCount
FROM baksteballVenues
GROUP BY 
    CASE 
        WHEN Capacity < 15000 THEN 'Small'
        WHEN Capacity BETWEEN 15000 AND 20000 THEN 'Medium'
        ELSE 'Large'
    END
ORDER BY VenueCount DESC;