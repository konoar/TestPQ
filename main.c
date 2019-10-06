/****************************************************************

   main.c
     Copyright 2019.10.05 konoar

 ****************************************************************/
#include "libpq-fe.h"
#include <stdio.h>
#include <stdlib.h>

#define KS_OK 0
#define KS_NG 1

int ksInsert(PGconn *conn)
{

    PGresult *res = NULL;
    int retval = KS_OK;

    do {

        // BEGIN TRANSACTION

        res = PQexec(conn,
            "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE");

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

        PQclear(res);
        res = NULL;

        // INSERT
 
        res = PQexec(conn,
                "INSERT INTO TestPQ.R_TEST(value)"
                "values ('eee'), ('fff')"
              );

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

        PQclear(res);
        res = NULL;

        // COMMIT

        res = PQexec(conn, "COMMIT");

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

    } while(0);

    if (res) {
        PQclear(res);
        res = NULL;
    }

    return retval;

}

 int ksSelectA(PGconn *conn)
 {

    PGresult *res = NULL;
    int retval = KS_OK, ret;

    putc('\n', stdout);

    do {

        // BEGIN TRANSACTION

        res = PQexec(conn,
            "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY");

        if (PGRES_COMMAND_OK != (ret = PQresultStatus(res))) {
            retval = KS_NG;
            break;
        }

        PQclear(res);
        res = NULL;

        // DECLARE CURSOR

        res = PQexec(conn,
            "DECLARE mycursor CURSOR FOR "
              "SELECT "
                "rid,"
                "tid,"
                "value "
              "FROM "
                "TestPQ.F_SELECT_TEST(LOCALTIMESTAMP)"
            );

        if (PGRES_COMMAND_OK != (ret = PQresultStatus(res))) {
            retval = KS_NG;
            break;
        }

        PQclear(res);
        res = NULL;

        // FETCH

        res = PQexec(conn, "FETCH ALL IN mycursor");

        if (PGRES_TUPLES_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

        for (int idx = 0; idx < PQntuples(res); idx++) {
            printf("%-10s%-16s%-16s\n",
                PQgetvalue(res, idx, 0),
                PQgetvalue(res, idx, 1),
                PQgetvalue(res, idx, 2));
        }

        PQclear(res);
        res = NULL;

        // END TRANSACTION

        res = PQexec(conn, "END TRANSACTION");

        if (PGRES_COMMAND_OK != (ret = PQresultStatus(res))) {
            retval = KS_NG;
            break;
        }

    } while(0);

    if (res) {
        PQclear(res);
        res = NULL;
    }

    putc('\n', stdout);

    return retval;

 }

 int ksSelectB(PGconn *conn)
 {

    PGresult *res = NULL;
    int retval = KS_OK, ret;

    putc('\n', stdout);

    do {

        // BEGIN TRANSACTION

        res = PQexec(conn,
            "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY");

        if (PGRES_COMMAND_OK != (ret = PQresultStatus(res))) {
            retval = KS_NG;
            break;
        }

        PQclear(res);
        res = NULL;

        // SELECT

        if (0 == PQsendQuery(conn,
            "SELECT "
              "rid,"
              "tid,"
              "value "
            "FROM "
              "TestPQ.F_SELECT_TEST(LOCALTIMESTAMP)"
            )) {

            retval = KS_NG;
            break;
        }

        PQsetSingleRowMode(conn);

        while (res = PQgetResult(conn)) {

            if (PGRES_SINGLE_TUPLE == PQresultStatus(res)) {
                printf("%-10s%-16s%-16s\n",
                    PQgetvalue(res, 0, 0),
                    PQgetvalue(res, 0, 1),
                    PQgetvalue(res, 0, 2));
            }

            PQclear(res);
            res = NULL;

        }

        // END TRANSACTION

        res = PQexec(conn, "END TRANSACTION");

        if (PGRES_COMMAND_OK != (ret = PQresultStatus(res))) {
            retval = KS_NG;
            break;
        }

    } while(0);

    if (res) {
        PQclear(res);
        res = NULL;
    }

    putc('\n', stdout);

    return retval;

 }

int main(int argc, const  char* argv[])
{

    int ret = KS_OK;

    char buff[256];
    PGconn *conn;

    sprintf(
        buff, "postgresql://%s@localhost:5432/%s",
        getenv("USER"), getenv("USER"));

    conn = PQconnectdb(buff);

    if (CONNECTION_OK != PQstatus(conn)) {
        return KS_NG;
    }

    do {

        if (KS_OK != (ret = ksInsert(conn))) {
            break;
        }

        if (KS_OK != (ret = ksSelectB(conn))) {
            break;
        }

    } while(0);

    PQfinish(conn);

    return ret;

}
