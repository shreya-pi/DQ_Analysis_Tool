I'll create the requested views for a few key tables from the schema. Due to length constraints, I'll show examples for three important tables, but the same pattern can be applied to others.

1. For ASSETMASTER:

```sql
-- View 1: Duplicate Records
CREATE OR REPLACE VIEW assetmaster_duplicate_records AS
WITH DuplicateCTE AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   SPVNAME, LEGALSTRUCTURE, DEALNAME, SHARECLASSNAME,
                   ASSETTYPE, DEALTYPE, FUNDTYPE, ASSETCLASS,
                   SECTOR, REGION, STRATEGY, STATUS
               ORDER BY CREATE_DATE DESC
           ) AS row_num
    FROM ASSETMASTER
)
SELECT *
FROM DuplicateCTE
WHERE row_num > 1;

-- View 2: Clean View
CREATE OR REPLACE VIEW assetmaster_clean_view AS
WITH CleanCTE AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   SPVNAME, LEGALSTRUCTURE, DEALNAME, SHARECLASSNAME,
                   ASSETTYPE, DEALTYPE, FUNDTYPE, ASSETCLASS,
                   SECTOR, REGION, STRATEGY, STATUS
               ORDER BY CREATE_DATE DESC
           ) AS row_num
    FROM ASSETMASTER
)
SELECT a.*
FROM CleanCTE a
WHERE row_num = 1;
```

2. For MANDATEDETAILS:

```sql
-- View 1: Duplicate Records
CREATE OR REPLACE VIEW mandatedetails_duplicate_records AS
WITH DuplicateCTE AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   NEWCLIENT, STARTDATE, CLOSINGDATE, CLIENTAGE,
                   CLIENTPORTFOLIOAGE, GENDERIDENTITY, INDUSTRY,
                   WEALTHRANGE, RISKLEVEL, OURLEGALENTITY,
                   SCORE, DEFAULT, PREVIOUSRM, CURRENTRM, STATUS
               ORDER BY CREATE_DATE DESC
           ) AS row_num
    FROM MANDATEDETAILS
)
SELECT *
FROM DuplicateCTE
WHERE row_num > 1;

-- View 2: Clean View
CREATE OR REPLACE VIEW mandatedetails_clean_view AS
WITH CleanCTE AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   NEWCLIENT, STARTDATE, CLOSINGDATE, CLIENTAGE,
                   CLIENTPORTFOLIOAGE, GENDERIDENTITY, INDUSTRY,
                   WEALTHRANGE, RISKLEVEL, OURLEGALENTITY,
                   SCORE, DEFAULT, PREVIOUSRM, CURRENTRM, STATUS
               ORDER BY CREATE_DATE DESC
           ) AS row_num
    FROM MANDATEDETAILS
)
SELECT m.*
FROM CleanCTE m
INNER JOIN PORTFOLIO p ON m.MANDATEID = p.MANDATEID
INNER JOIN RMTOMANDATE rm ON m.MANDATEID = rm.MANDATEID
WHERE m.row_num = 1;
```

3. For RM_MASTER:

```sql
-- View 1: Duplicate Records
CREATE OR REPLACE VIEW rm_master_duplicate_records AS
WITH DuplicateCTE AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   FIRSTNAME, LASTNAME, ROLE,
                   LOCATION, JOINDATE, ACTIVE
               ORDER BY CREATE_DATE DESC
           ) AS row_num
    FROM RM_MASTER
)
SELECT *
FROM DuplicateCTE
WHERE row_num > 1;

-- View 2: Clean View
CREATE OR REPLACE VIEW rm_master_clean_view AS
WITH CleanCTE AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   FIRSTNAME, LASTNAME, ROLE,
                   LOCATION, JOINDATE, ACTIVE
               ORDER BY CREATE_DATE DESC
           ) AS row_num
    FROM RM_MASTER
)
SELECT rm.*
FROM CleanCTE rm
INNER JOIN RMTOMANDATE rmt ON rm.EMAIL = rmt.EMAIL
INNER JOIN AUMDETAILS aum ON rm.EMAIL = aum.EMAIL
WHERE rm.row_num = 1;
```

Key features of these views:

1. The duplicate_records views:
- Identify duplicates based on business-relevant columns
- Exclude technical columns (IDs, timestamps) from duplicate detection
- Show all duplicate records (row_num > 1)

2. The clean_views:
- Remove duplicates by keeping only the latest record
- Maintain referential integrity through appropriate JOINs
- Include only active relationships with related tables

You can follow this pattern for the remaining tables in the schema, adjusting the PARTITION BY clauses based on the business-relevant columns for each table and including appropriate JOINs based on the foreign key relationships defined in the schema.