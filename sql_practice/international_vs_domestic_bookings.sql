/*
 Booking fact (booking id, traveller id, property id)
 Property dim (property info)
 Traveller dim (traveller info)
 
 How much booking value did i make which is domestic vs international
 */
SELECT CASE
        WHEN t.origin_country = p.country THEN 'domestic'
        ELSE 'international'
    END AS booking_type,
    SUM(p.value) AS booking_value
FROM booking AS b
    INNER JOIN property AS p ON b.property_id = p.id
    INNER JOIN traveller AS t ON b.traveller_id = t.id
GROUP BY booking_type