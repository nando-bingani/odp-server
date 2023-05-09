-- search_records()
EXPLAIN
SELECT count(*) AS count_1
FROM (SELECT 1
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND catalog_record.searchable) AS anon_1;

EXPLAIN
SELECT 1
FROM catalog_record
WHERE catalog_record.catalog_id = :catalog_id_1
  AND catalog_record.published
  AND catalog_record.searchable
ORDER BY catalog_record.timestamp DESC
LIMIT :param_1 OFFSET :param_2;

EXPLAIN
SELECT anon_1.facet, anon_1.value, count(*) AS count_1
FROM (SELECT catalog_record.catalog_id AS catalog_id,
             catalog_record.record_id  AS record_id
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND catalog_record.searchable) AS anon_2
         JOIN (SELECT *
               FROM catalog_record_facet) AS anon_1 ON anon_2.catalog_id = anon_1.catalog_id AND anon_2.record_id = anon_1.record_id
GROUP BY anon_1.facet, anon_1.value;

-- search_records(text_query)
EXPLAIN
SELECT count(*) AS count_1
FROM (SELECT 1
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND full_text @@ plainto_tsquery('english', :text_query)) AS anon_1;

EXPLAIN
SELECT 1
FROM catalog_record
WHERE catalog_record.catalog_id = :catalog_id_1
  AND catalog_record.published
  AND full_text @@ plainto_tsquery('english', :text_query)
ORDER BY catalog_record.timestamp DESC
LIMIT :param_1 OFFSET :param_2;

EXPLAIN
SELECT anon_1.facet, anon_1.value, count(*) AS count_1
FROM (SELECT catalog_record.catalog_id AS catalog_id,
             catalog_record.record_id  AS record_id
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND full_text @@ plainto_tsquery('english', :text_query)) AS anon_2
         JOIN (SELECT *
               FROM catalog_record_facet) AS anon_1 ON anon_2.catalog_id = anon_1.catalog_id AND anon_2.record_id = anon_1.record_id
GROUP BY anon_1.facet, anon_1.value;

-- search_records(facet_query(instrument))
EXPLAIN
SELECT count(*) AS count_1
FROM (SELECT 1
      FROM catalog_record
               JOIN catalog_record_facet AS "crfInstrument"
                    ON catalog_record.catalog_id = "crfInstrument".catalog_id AND catalog_record.record_id = "crfInstrument".record_id
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND "crfInstrument".facet = :facet_1
        AND "crfInstrument".value = :value_1) AS anon_1;

EXPLAIN
SELECT 1
FROM catalog_record
         JOIN catalog_record_facet AS "crfInstrument"
              ON catalog_record.catalog_id = "crfInstrument".catalog_id AND catalog_record.record_id = "crfInstrument".record_id
WHERE catalog_record.catalog_id = :catalog_id_1
  AND catalog_record.published
  AND "crfInstrument".facet = :facet_1
  AND "crfInstrument".value = :value_1
ORDER BY catalog_record.timestamp DESC
LIMIT :param_1 OFFSET :param_2;

EXPLAIN
SELECT anon_1.facet, anon_1.value, count(*) AS count_1
FROM (SELECT catalog_record.catalog_id AS catalog_id,
             catalog_record.record_id  AS record_id
      FROM catalog_record
               JOIN catalog_record_facet AS "crfInstrument"
                    ON catalog_record.catalog_id = "crfInstrument".catalog_id AND catalog_record.record_id = "crfInstrument".record_id
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND "crfInstrument".facet = :facet_1
        AND "crfInstrument".value = :value_1) AS anon_2
         JOIN (SELECT *
               FROM catalog_record_facet) AS anon_1 ON anon_2.catalog_id = anon_1.catalog_id AND anon_2.record_id = anon_1.record_id
GROUP BY anon_1.facet, anon_1.value;

-- search_records(facet_query(instrument, location))
EXPLAIN
SELECT count(*) AS count_1
FROM (SELECT 1
      FROM catalog_record
               JOIN catalog_record_facet AS "crfInstrument"
                    ON catalog_record.catalog_id = "crfInstrument".catalog_id AND catalog_record.record_id = "crfInstrument".record_id
               JOIN catalog_record_facet AS "crfLocation"
                    ON catalog_record.catalog_id = "crfLocation".catalog_id AND catalog_record.record_id = "crfLocation".record_id
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND "crfInstrument".facet = :facet_1
        AND "crfInstrument".value = :value_1
        AND "crfLocation".facet = :facet_2
        AND "crfLocation".value = :value_2) AS anon_1;

EXPLAIN
SELECT 1
FROM catalog_record
         JOIN catalog_record_facet AS "crfInstrument"
              ON catalog_record.catalog_id = "crfInstrument".catalog_id AND catalog_record.record_id = "crfInstrument".record_id
         JOIN catalog_record_facet AS "crfLocation"
              ON catalog_record.catalog_id = "crfLocation".catalog_id AND catalog_record.record_id = "crfLocation".record_id
WHERE catalog_record.catalog_id = :catalog_id_1
  AND catalog_record.published
  AND "crfInstrument".facet = :facet_1
  AND "crfInstrument".value = :value_1
  AND "crfLocation".facet = :facet_2
  AND "crfLocation".value = :value_2
ORDER BY catalog_record.timestamp DESC
LIMIT :param_1 OFFSET :param_2;

EXPLAIN
SELECT anon_1.facet, anon_1.value, count(*) AS count_1
FROM (SELECT catalog_record.catalog_id AS catalog_id,
             catalog_record.record_id  AS record_id
      FROM catalog_record
               JOIN catalog_record_facet AS "crfInstrument"
                    ON catalog_record.catalog_id = "crfInstrument".catalog_id AND catalog_record.record_id = "crfInstrument".record_id
               JOIN catalog_record_facet AS "crfLocation"
                    ON catalog_record.catalog_id = "crfLocation".catalog_id AND catalog_record.record_id = "crfLocation".record_id
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND "crfInstrument".facet = :facet_1
        AND "crfInstrument".value = :value_1
        AND "crfLocation".facet = :facet_2
        AND "crfLocation".value = :value_2) AS anon_2
         JOIN (SELECT *
               FROM catalog_record_facet) AS anon_1 ON anon_2.catalog_id = anon_1.catalog_id AND anon_2.record_id = anon_1.record_id
GROUP BY anon_1.facet, anon_1.value;

-- search_records(n, s, e, w)
-- note that in this non-exclusive region case, the param names produced by
-- SQLA are mismatched with the API params; i.e. :spatial_south_1 takes the
-- north_bound API param, etc
EXPLAIN
SELECT count(*) AS count_1
FROM (SELECT 1
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND catalog_record.spatial_south <= :spatial_south_1
        AND catalog_record.spatial_north >= :spatial_north_1
        AND catalog_record.spatial_west <= :spatial_west_1
        AND catalog_record.spatial_east >= :spatial_east_1) AS anon_1;

EXPLAIN
SELECT 1
FROM catalog_record
WHERE catalog_record.catalog_id = :catalog_id_1
  AND catalog_record.published
  AND catalog_record.spatial_south <= :spatial_south_1
  AND catalog_record.spatial_north >= :spatial_north_1
  AND catalog_record.spatial_west <= :spatial_west_1
  AND catalog_record.spatial_east >= :spatial_east_1
ORDER BY catalog_record.timestamp DESC
LIMIT :param_1 OFFSET :param_2;

EXPLAIN
SELECT anon_1.facet, anon_1.value, count(*) AS count_1
FROM (SELECT catalog_record.catalog_id AS catalog_id,
             catalog_record.record_id  AS record_id
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND catalog_record.spatial_south <= :spatial_south_1
        AND catalog_record.spatial_north >= :spatial_north_1
        AND catalog_record.spatial_west <= :spatial_west_1
        AND catalog_record.spatial_east >= :spatial_east_1) AS anon_2
         JOIN (SELECT *
               FROM catalog_record_facet) AS anon_1 ON anon_2.catalog_id = anon_1.catalog_id AND anon_2.record_id = anon_1.record_id
GROUP BY anon_1.facet, anon_1.value;

-- search_records(n, s, e, w, exclusive_region)
EXPLAIN
SELECT count(*) AS count_1
FROM (SELECT 1
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND catalog_record.spatial_north <= :spatial_north_1
        AND catalog_record.spatial_south >= :spatial_south_1
        AND catalog_record.spatial_east <= :spatial_east_1
        AND catalog_record.spatial_west >= :spatial_west_1) AS anon_1;

EXPLAIN
SELECT 1
FROM catalog_record
WHERE catalog_record.catalog_id = :catalog_id_1
  AND catalog_record.published
  AND catalog_record.spatial_north <= :spatial_north_1
  AND catalog_record.spatial_south >= :spatial_south_1
  AND catalog_record.spatial_east <= :spatial_east_1
  AND catalog_record.spatial_west >= :spatial_west_1
ORDER BY catalog_record.timestamp DESC
LIMIT :param_1 OFFSET :param_2;

EXPLAIN
SELECT anon_1.facet, anon_1.value, count(*) AS count_1
FROM (SELECT catalog_record.catalog_id AS catalog_id,
             catalog_record.record_id  AS record_id
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND catalog_record.spatial_north <= :spatial_north_1
        AND catalog_record.spatial_south >= :spatial_south_1
        AND catalog_record.spatial_east <= :spatial_east_1
        AND catalog_record.spatial_west >= :spatial_west_1) AS anon_2
         JOIN (SELECT *
               FROM catalog_record_facet) AS anon_1 ON anon_2.catalog_id = anon_1.catalog_id AND anon_2.record_id = anon_1.record_id
GROUP BY anon_1.facet, anon_1.value;

-- search_records(start, end)
-- :coalesce_1 takes the start_date API param
-- :temporal_start_1 takes the end_date API param
EXPLAIN
SELECT count(*) AS count_1
FROM (SELECT 1
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND coalesce(catalog_record.temporal_end, catalog_record.temporal_start) >= :coalesce_1
        AND catalog_record.temporal_start <= :temporal_start_1) AS anon_1;

EXPLAIN
SELECT 1
FROM catalog_record
WHERE catalog_record.catalog_id = :catalog_id_1
  AND catalog_record.published
  AND coalesce(catalog_record.temporal_end, catalog_record.temporal_start) >= :coalesce_1
  AND catalog_record.temporal_start <= :temporal_start_1
ORDER BY catalog_record.timestamp DESC
LIMIT :param_1 OFFSET :param_2;

EXPLAIN
SELECT anon_1.facet, anon_1.value, count(*) AS count_1
FROM (SELECT catalog_record.catalog_id AS catalog_id,
             catalog_record.record_id  AS record_id
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND coalesce(catalog_record.temporal_end, catalog_record.temporal_start) >= :coalesce_1
        AND catalog_record.temporal_start <= :temporal_start_1) AS anon_2
         JOIN (SELECT *
               FROM catalog_record_facet) AS anon_1 ON anon_2.catalog_id = anon_1.catalog_id AND anon_2.record_id = anon_1.record_id
GROUP BY anon_1.facet, anon_1.value;

-- search_records(start, end, exclusive_interval)
-- :temporal_start_1 takes the start_date API param
-- :coalesce_1 takes the end_date API param
EXPLAIN
SELECT count(*) AS count_1
FROM (SELECT 1
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND catalog_record.temporal_start >= :temporal_start_1
        AND coalesce(catalog_record.temporal_end, catalog_record.temporal_start) <= :coalesce_1) AS anon_1;

EXPLAIN
SELECT 1
FROM catalog_record
WHERE catalog_record.catalog_id = :catalog_id_1
  AND catalog_record.published
  AND catalog_record.temporal_start >= :temporal_start_1
  AND coalesce(catalog_record.temporal_end, catalog_record.temporal_start) <= :coalesce_1
ORDER BY catalog_record.timestamp DESC
LIMIT :param_1 OFFSET :param_2;

EXPLAIN
SELECT anon_1.facet, anon_1.value, count(*) AS count_1
FROM (SELECT catalog_record.catalog_id AS catalog_id,
             catalog_record.record_id  AS record_id
      FROM catalog_record
      WHERE catalog_record.catalog_id = :catalog_id_1
        AND catalog_record.published
        AND catalog_record.temporal_start >= :temporal_start_1
        AND coalesce(catalog_record.temporal_end, catalog_record.temporal_start) <= :coalesce_1) AS anon_2
         JOIN (SELECT *
               FROM catalog_record_facet) AS anon_1 ON anon_2.catalog_id = anon_1.catalog_id AND anon_2.record_id = anon_1.record_id
GROUP BY anon_1.facet, anon_1.value;
