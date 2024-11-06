-- Create SYSTEM_LKUP table
CREATE OR REPLACE TABLE SYSTEM_LKUP (
    REC_SYS_ID INT,
    MPI_TPC_PRD_PLATFORM STRING
);

-- Insert dummy data into SYSTEM_LKUP
INSERT INTO SYSTEM_LKUP (REC_SYS_ID, MPI_TPC_PRD_PLATFORM) VALUES
    (1, 'Rise_EOS'),
    (2, 'Elastic_EOS'),
    (3, 'SWL_EOS'),
    (4, 'SWL_DDS'),
    (5, 'SWL_AMS');

-- Create ENT_PRSN_ETL_INC table
CREATE OR REPLACE TABLE ENT_PRSN_ETL_INC (
    PersonID INT,
    AccountNumber STRING,
    Platform STRING,
    Code STRING,
    MapID INT,
    StartDate DATE,
    BatchID INT,
    InsertTimestamp TIMESTAMP
);

-- Insert dummy data into ENT_PRSN_ETL_INC
INSERT INTO ENT_PRSN_ETL_INC (PersonID, AccountNumber, Platform, Code, MapID, StartDate, BatchID, InsertTimestamp) VALUES
    (101, 'ACC123', 'Rise_EOS', 'Code1', 1, '2023-01-01', 1, CURRENT_TIMESTAMP),
    (102, 'ACC124', 'Elastic_EOS', 'Code2', 2, '2023-01-02', 1, CURRENT_TIMESTAMP),
    (103, 'ACC125', 'SWL_EOS', 'Code3', 3, '2023-01-03', 1, CURRENT_TIMESTAMP),
    (104, 'ACC126', 'SWL_DDS', 'Code4', 4, '2023-01-04', 1, CURRENT_TIMESTAMP),
    (105, 'ACC127', 'SWL_AMS', 'Code5', 5, '2023-01-05', 1, CURRENT_TIMESTAMP);

-- Create ENT_PRSN table
CREATE OR REPLACE TABLE ENT_PRSN (
    PersonID INT,
    AccountNumber STRING,
    Platform STRING,
    Code STRING,
    MapID INT,
    StartDate DATE,
    SystemID INT,
    ChangeOperation STRING,
    BatchID INT,
    InsertTimestamp TIMESTAMP,
    UpdateTimestamp TIMESTAMP
);

-- Insert dummy data into ENT_PRSN
INSERT INTO ENT_PRSN (PersonID, AccountNumber, Platform, Code, MapID, StartDate, SystemID, ChangeOperation, BatchID, InsertTimestamp, UpdateTimestamp) VALUES
    (201, 'ACC123', 'Rise_EOS', 'Code1', 1, '2023-01-01', 1, 'U', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    (202, 'ACC124', 'Elastic_EOS', 'Code2', 2, '2023-01-02', 2, 'U', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    (203, 'ACC125', 'SWL_EOS', 'Code3', 3, '2023-01-03', 3, 'U', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Verify the data
SELECT * FROM SYSTEM_LKUP;
SELECT * FROM ENT_PRSN_ETL_INC;
SELECT * FROM ENT_PRSN;

-- Set the query tag for tracking
ALTER SESSION SET QUERY_TAG = 'shr_cmb_ent_prsn.sql';

-- Create TEMP_SYSTEM_LKUP table
CREATE OR REPLACE TEMPORARY TABLE TEMP_SYSTEM_LKUP AS (
    SELECT REC_SYS_ID AS SystemID,
           MPI_TPC_PRD_PLATFORM AS Platform,
           CAST(REC_SYS_ID * 1000000000 AS BIGINT) AS AccountPrefix,
           1 AS AccountSign
    FROM SYSTEM_LKUP
);

-- Insert test data into TEMP_SYSTEM_LKUP
INSERT INTO TEMP_SYSTEM_LKUP (SystemID, Platform, AccountPrefix, AccountSign) VALUES 
    (5, 'Rise_EOS', 55000000000, -1),
    (3, 'Elastic_EOS', 33000000000, -1),
    (42, 'SWL_EOS', 42000000000, -1),
    (42, 'SWL_DDS', 42000000000, -1),
    (42, 'SWL_AMS', 42000000000, -1);

-- Create TEMP_ENT_PERSON table
CREATE OR REPLACE TEMPORARY TABLE TEMP_ENT_PERSON AS (
    SELECT
        EI.PersonID,
        EI.AccountNumber,
        EI.Platform,
        EI.Code,
        EI.MapID,
        EI.StartDate,
        SYS.SystemID,
        CASE
            WHEN EP.AccountNumber IS NULL THEN 'I'
            ELSE 'U'
        END AS ChangeOperation,
        EI.BatchID,
        EI.InsertTimestamp,
        EI.InsertTimestamp AS UpdateTimestamp,
        ROW_NUMBER() OVER (PARTITION BY EI.Platform, EI.AccountNumber ORDER BY EI.InsertTimestamp DESC NULLS LAST) AS RN
    FROM ENT_PRSN_ETL_INC EI
    LEFT JOIN TEMP_SYSTEM_LKUP SYS ON SYS.Platform = EI.Platform
    LEFT JOIN ENT_PRSN EP ON EP.AccountNumber = EI.AccountNumber AND EP.MapID = EI.MapID
    QUALIFY RN = 1
);

-- Merge data for updates
MERGE INTO ENT_PRSN AS A
USING (
    SELECT
        A.PersonID,
        A.AccountNumber,
        A.Platform,
        A.Code,
        A.MapID,
        A.StartDate,
        A.SystemID,
        A.ChangeOperation,
        A.BatchID,
        A.UpdateTimestamp
    FROM TEMP_ENT_PERSON A
    INNER JOIN ENT_PRSN B ON A.AccountNumber = B.AccountNumber AND A.MapID = B.MapID
    WHERE A.ChangeOperation = 'U'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY A.AccountNumber, A.Platform ORDER BY A.AccountNumber, A.Platform) = 1
) AS B ON A.AccountNumber = B.AccountNumber AND A.Platform = B.Platform
WHEN MATCHED THEN UPDATE SET
    A.PersonID = B.PersonID,
    A.Code = B.Code,
    A.MapID = B.MapID,
    A.StartDate = B.StartDate,
    A.SystemID = B.SystemID,
    A.ChangeOperation = B.ChangeOperation,
    A.BatchID = B.BatchID,
    A.UpdateTimestamp = B.UpdateTimestamp;

-- Merge data for inserts
MERGE INTO ENT_PRSN AS A
USING (
    SELECT
        A.AccountNumber,
        A.PersonID,
        A.Platform,
        A.Code,
        A.MapID,
        A.StartDate,
        A.SystemID,
        A.ChangeOperation,
        A.BatchID,
        A.InsertTimestamp,
        A.UpdateTimestamp
    FROM TEMP_ENT_PERSON A
    WHERE A.ChangeOperation = 'I'
) AS B ON A.AccountNumber = B.AccountNumber AND A.Platform = B.Platform
WHEN NOT MATCHED THEN INSERT (
    PersonID,
    AccountNumber,
    Platform,
    Code,
    MapID,
    StartDate,
    SystemID,
    ChangeOperation,
    BatchID,
    InsertTimestamp,
    UpdateTimestamp
) VALUES (
    B.PersonID,
    B.AccountNumber,
    B.Platform,
    B.Code,
    B.MapID,
    B.StartDate,
    B.SystemID,
    B.ChangeOperation,
    B.BatchID,
    B.InsertTimestamp,
    B.UpdateTimestamp
);

-- Commit the transaction
COMMIT;

-- Verify the results
SELECT * FROM ENT_PRSN;