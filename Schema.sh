#!/bin/bash

#########################################################
#
# Schema.sh
#   Copyright 2019.10.05 konoar
#
#########################################################

# Drop
psql -U postgres <<END
DROP DATABASE IF EXISTS ${USER};
DROP ROLE     IF EXISTS ${USER};
END

# Create
psql -U postgres <<END
CREATE ROLE     ${USER} WITH LOGIN;
CREATE DATABASE ${USER} WITH OWNER ${USER};
END

psql -U ${USER}  <<END

CREATE SCHEMA TestPQ;

CREATE SEQUENCE TestPQ.S_TEST START WITH 1 INCREMENT BY 1;

CREATE FUNCTION TestPQ.F_GET_DEFAULT_TID() RETURNS cHAR(8) AS
\$\$

    SELECT 'TID' || TO_CHAR(NEXTVAL('TestPQ.S_TEST'), 'FM00000');

\$\$ LANGUAGE SQL;

CREATE TABLE TestPQ.R_TEST
(
    tid     CHAR(8) PRIMARY KEY DEFAULT TestPQ.F_GET_DEFAULT_TID(),
    value   VARCHAR(16) NOT NULL,
    ctime   TIMESTAMP DEFAULT LOCALTIMESTAMP
)
WITH
(
    FILLFACTOR = 90
);

CREATE INDEX ON TestPQ.R_TEST
(
    value
);

CREATE FUNCTION TestPQ.F_SELECT_TEST(target TIMESTAMP)
    RETURNS TABLE(rid BIGINT, tid CHAR(8), value VARCHAR(16)) AS
\$\$

    SELECT
        ROW_NUMBER() OVER w1    AS rid,
        r1.tid                  AS tid,
        r1.value                AS value
    FROM
        TestPQ.R_TEST r1
    WHERE
        r1.ctime <= target 
    WINDOW
        w1 AS(ORDER BY r1.value ASC)

\$\$ LANGUAGE SQL;

INSERT INTO TestPQ.R_TEST
(
    value,
    ctime
)
VALUES
    ('bbb', CAST(CURRENT_DATE - 2 AS TIMESTAMP)),
    ('ccc', CAST(CURRENT_DATE - 1 AS TIMESTAMP)),
    ('aaa', CAST(CURRENT_DATE - 0 AS TIMESTAMP)),
    ('ddd', CAST(CURRENT_DATE + 1 AS TIMESTAMP));

END

