
ALTER SESSION SET QUERY_TAG = 'shr_cmb_ent_prsn.sql';

-- Create TEMP_SYSTEM_LKUP table
CREATE TEMPORARY TABLE TEMP_SYSTEM_LKUP AS (
    SELECT REC_SYS_ID AS SystemID,
           MPI_TPC_PRD_PLATFORM AS Platform,
           CAST(REC_SYS_ID * 1000000000 AS BigInt) AS AccountPrefix,
           1 AS AccountSign
    FROM "${CMB_STG_SHR_DB}".SYSTEM_LKUP
);

-- Insert test data into TEMP_SYSTEM_LKUP
INSERT INTO TEMP_SYSTEM_LKUP (SystemID, Platform, AccountPrefix, AccountSign) VALUES 
    (5, 'Rise_EOS', 55000000000, -1),
    (3, 'Elastic_EOS', 33000000000, -1),
    (42, 'SWL_EOS', 42000000000, -1),
    (42, 'SWL_DDS', 42000000000, -1),
    (42, 'SWL_AMS', 42000000000, -1);

-- Create TEMP_ENT_PERSON table
CREATE TEMPORARY TABLE TEMP_ENT_PERSON AS (
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
    FROM "${CMB_STG_SHR_DB}".ENT_PRSN_ETL_INC EI
    LEFT JOIN TEMP_SYSTEM_LKUP SYS ON SYS.Platform = EI.Platform
    LEFT JOIN "${CMB_STG_SHR_DB}".ENT_PRSN EP ON EP.AccountNumber = EI.AccountNumber AND EP.MapID = EI.MapID
    QUALIFY RN = 1
);

-- Merge data for updates
MERGE INTO ${CMB_STG_SHR_DB}.ENT_PRSN A 
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
    INNER JOIN "${CMB_STG_SHR_DB}".ENT_PRSN B ON A.AccountNumber = B.AccountNumber AND A.MapID = B.MapID
    WHERE A.ChangeOperation = 'U'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY A.AccountNumber, A.Platform ORDER BY A.AccountNumber, A.Platform) = 1
) B ON A.AccountNumber = B.AccountNumber AND A.Platform = B.Platform
WHEN MATCHED THEN UPDATE SET
    PersonID = B.PersonID,
    Code = B.Code,
    MapID = B.MapID,
    StartDate = B.StartDate,
    SystemID = B.SystemID,
    ChangeOperation = B.ChangeOperation,
    BatchID = B.BatchID,
    UpdateTimestamp = B.UpdateTimestamp;

-- Merge data for inserts
MERGE INTO ${CMB_STG_SHR_DB}.ENT_PRSN A 
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
) B ON A.AccountNumber = B.AccountNumber AND A.Platform = B.Platform
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

COMMIT;

-- Verify the results
SELECT * FROM ${CMB_STG_SHR_DB}.ENT_PRSN;

