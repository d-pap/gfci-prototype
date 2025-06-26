-- creates schemas and db structure
-- only run once at the beginning or when resetting everything

-- BRONZE SCHEMA: one table per source, keep as-is
-- SILVER SCHEMA: one table per DOMAIN (e.g., jobs, rents, etc.), standardize and combine where it makes sense
-- GOLD SCHEMA: final answers, joined and aggregated across domains (e.g., city job score, job posting trends, etc.)

-- create schemas
create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;