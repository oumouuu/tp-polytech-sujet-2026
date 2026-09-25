-- Idée de requêtes d'analyse pour valider votre modèle gold.

-- Les requêtes "métier" ne sont pas implémenté pour ne pas
-- donner trop d'indices sur le data modèle attendu. Mais vous
-- êtes libre d'essayer d'implémenter ces requêtes pour valider
-- votre conception.

-- Requêtes écrites sur le modèle gold (fact_booking + dim_flight + dim_passenger + dim_airport
-- + dim_currency) pour vérifier qu'il répond simplement à chaque question. Chaque requête est
-- séparée par un point-virgule : le fichier peut être exécuté d'un bloc, ou requête par requête
-- (ex. depuis Python : duckdb.connect("warehouse.duckdb").sql("...")).

-- 1. Chiffre d'affaires par mois et par devise
SELECT
    strftime(booking_date, '%Y-%m') AS month,
    currency,
    COUNT(*)              AS nb_bookings,
    ROUND(SUM(amount), 2) AS revenue
FROM fact_booking
GROUP BY month, currency
ORDER BY month, currency;

-- 2. Top 10 des compagnies aériennes par chiffre d'affaires (en EUR, pour comparer des devises différentes)
SELECT
    d.airline,
    COUNT(*)                  AS nb_bookings,
    ROUND(SUM(f.amount_eur), 2) AS revenue_eur
FROM fact_booking f
JOIN dim_flight d ON f.flight_id = d.flight_id
GROUP BY d.airline
ORDER BY revenue_eur DESC
LIMIT 10;

-- 3. Répartition des réservations par classe de cabine
SELECT
    seat_class,
    COUNT(*) AS nb_bookings,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM fact_booking
GROUP BY seat_class
ORDER BY nb_bookings DESC;

-- 4. Volume de réservations par jour de la semaine
SELECT
    isodow(booking_date)   AS weekday_num,   -- 1 = lundi ... 7 = dimanche
    dayname(booking_date)  AS weekday,
    COUNT(*)               AS nb_bookings
FROM fact_booking
GROUP BY weekday_num, weekday
ORDER BY weekday_num;

-- 5. Top 10 des aéroports de départ générant le plus de réservations
-- (airport_id sur une réservation = origin_airport_id du vol réservé)
SELECT
    a.iata_code,
    a.airport_name,
    a.city,
    COUNT(*) AS nb_bookings
FROM fact_booking f
JOIN dim_airport a ON f.airport_id = a.airport_id
GROUP BY a.iata_code, a.airport_name, a.city
ORDER BY nb_bookings DESC
LIMIT 10;

-- 6. Évolution quotidienne du nombre de réservations sur septembre
SELECT
    booking_date,
    COUNT(*) AS nb_bookings
FROM fact_booking
WHERE booking_date BETWEEN DATE '2025-09-01' AND DATE '2025-09-30'
GROUP BY booking_date
ORDER BY booking_date;

-- 7. Chiffre d'affaires normalisé en EUR (total, puis détail par devise d'origine)
SELECT
    currency,
    ROUND(SUM(amount), 2)     AS revenue_original_currency,
    ROUND(SUM(amount_eur), 2) AS revenue_eur
FROM fact_booking
GROUP BY ROLLUP (currency)
ORDER BY currency NULLS LAST;

-- 8. Durée moyenne de vol par compagnie
SELECT
    airline,
    COUNT(*)                            AS nb_flights,
    ROUND(AVG(flight_duration_min), 0)  AS avg_duration_min,
    ROUND(AVG(flight_duration_min) / 60, 1) AS avg_duration_hours
FROM dim_flight
WHERE is_active
GROUP BY airline
ORDER BY avg_duration_min DESC;

-- 9. Chiffre d'affaires par tranche d'âge de passager
SELECT
    p.age_bracket,
    COUNT(*)                    AS nb_bookings,
    ROUND(SUM(f.amount_eur), 2) AS revenue_eur,
    ROUND(AVG(f.amount_eur), 2) AS avg_amount_eur
FROM fact_booking f
JOIN dim_passenger p ON f.passenger_id = p.passenger_id
GROUP BY p.age_bracket
ORDER BY p.age_bracket;
