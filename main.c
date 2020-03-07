/****************************************************************

   main.c
     Copyright 2019.10.05 konoar

 ****************************************************************/
#include "libpq-fe.h"
#include <pthread.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>

#define KS_OK               0
#define KS_NG               1

#define KS_READCOMMITED     0
#define KS_SERIALIZABLE     1
#define KS_ISOLATION_LEVEL  KS_READCOMMITED     

#if (KS_ISOLATION_LEVEL == KS_READCOMMITED)
//
// READ COMMITED
//
#define KS_BEGINTRANSACTION_READONLY    "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY"
#define KS_BEGINTRANSACTION_READWRITE   "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE"
#elif (KS_ISOLATION_LEVEL == KS_SERIALIZABLE)
//
// SERIALIZABLE
//
#define KS_BEGINTRANSACTION_READONLY    "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY"
#define KS_BEGINTRANSACTION_READWRITE   "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE"
#else
#error BAD ISOLATION LEVEL.
#endif

#define KS_SLEEP(_a_)                           \
    do {                                        \
                                                \
        struct timespec t = {                   \
            0, _a_ * 1000 * 1000                \
        };                                      \
        nanosleep(&t, NULL);                    \
                                                \
    } while(0)

#define KS_INSERT_COUNT 10

const char* ksInsertData[KS_INSERT_COUNT] =
{
    "eee",
    "fff",
    "ggg",
    "hhh",
    "iii",
    "jjj",
    "kkk",
    "lll",
    "mmm",
    "nnn"
};

typedef struct _ksInsertContext
{

    pthread_t   pth;
    long        retval;
    PGconn      *conn;
    const char  *data;

} ksInsertContext, *ksInsertContextPtr;

int ksConnect(PGconn **conn)
{

    char buff[256];

    if (!conn) {
        return KS_NG;
    }

    sprintf(buff, "postgresql://%s@%s:%s",
        getenv("USER"), getenv("PG_HOST"), getenv("PG_PORT"));

    *conn = PQconnectdb(buff);

    if (CONNECTION_OK != PQstatus(*conn)) {
        *conn = NULL;
        return KS_NG;
    }

    return KS_OK;

}

void *ksInsert(void *param)
{
 
    ksInsertContextPtr  ctx     = (ksInsertContextPtr)param;
    PGresult            *res    = NULL;
    int                 retval  = KS_OK;
    char                buff    [256];

    do {

        // BEGIN TRANSACTION

        res = PQexec(ctx->conn, KS_BEGINTRANSACTION_READWRITE);

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

        PQclear(res);
        res = NULL;

        // INSERT

        sprintf(buff,"INSERT INTO TestPQ.R_TEST(value) values ('%s')", ctx->data);
        res = PQexec(ctx->conn, buff);

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

        PQclear(res);
        res = NULL;

        // COMMIT

        srand((unsigned int)time(NULL));
        KS_SLEEP(rand() % 70);

        res = PQexec(ctx->conn, "COMMIT");

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

    } while(0);

    if (res) {
        PQclear(res);
        res = NULL;
    }

    return (void*)((long)retval);

}

void *ksUpdateSub(void *param, int pat)
{

    PGconn      *conn       = (PGconn*)param;
    PGresult    *res1       = NULL;
    PGresult    *res2       = NULL;
    long        retval      = KS_OK;
    char        buff        [256];

    do {

        // BEGIN TRANSACTION

        res1 = PQexec(conn, KS_BEGINTRANSACTION_READWRITE);

        if (PGRES_COMMAND_OK != PQresultStatus(res1)) {
            retval = KS_NG;
            break;
        }

        PQclear(res1);
        res1 = NULL;

        // DECLARE CURSOR

        if (1 == pat) {

            sprintf(buff,
                "DECLARE cursoru CURSOR FOR "
                  "SELECT  "
                    "tid,  "
                    "value "
                  "FROM    "
                    "TestPQ.R_TEST "
                  "ORDER BY value ASC "
                  "FOR UPDATE");

        } else {

            sprintf(buff,
                "DECLARE cursoru CURSOR FOR "
                  "SELECT  "
                    "tid,  "
                    "value "
                  "FROM    "
                    "TestPQ.R_TEST "
                  "ORDER BY value DESC "
                  "FOR UPDATE");

        }

        res1 = PQexec(conn, buff);

        if (PGRES_COMMAND_OK != PQresultStatus(res1)) {
            retval = KS_NG;
            break;
        }

        PQclear(res1);
        res1 = NULL;

        // FETCH

        res1 = PQexec(conn, "FETCH FIRST IN cursoru");

        if (PGRES_TUPLES_OK != PQresultStatus(res1)) {
            retval = KS_NG;
            break;
        }

        while (PGRES_TUPLES_OK == PQresultStatus(res1) && 1 == PQntuples(res1)) {

            // UPDATE

            if (1 == pat) {

                sprintf(buff,
                    "UPDATE TestPQ.R_TEST "
                      "SET value='%s_updated1' "
                    "WHERE CURRENT OF cursoru", PQgetvalue(res1, 0, 1));

            } else {

                sprintf(buff,
                    "UPDATE TestPQ.R_TEST "
                      "SET value='%s_updated2' "
                    "WHERE CURRENT OF cursoru", PQgetvalue(res1, 0, 1));

            }

            res2 = PQexec(conn, buff);

            if (PGRES_COMMAND_OK != PQresultStatus(res2)) {
                retval = KS_NG;
                break;
            }

            PQclear(res2);
            res2 = NULL;

            PQclear(res1);
            res1 = PQexec(conn, "FETCH NEXT IN cursoru");

        }

        PQclear(res1);
        res1 = NULL;

        if (KS_OK != retval) {
            break;
        }

        // COMMIT

        res1 = PQexec(conn, "COMMIT");

        if (PGRES_COMMAND_OK != PQresultStatus(res1)) {
            retval = KS_NG;
            break;
        }

    } while(0);

    if (res1) {
        PQclear(res1);
        res1 = NULL;
    }

    if (res2) {
        PQclear(res2);
        res2 = NULL;
    }

    return ((void*)retval);

}

void *ksUpdate1(void *param)
{

    return ksUpdateSub(param, 1);

}

void *ksUpdate2(void *param)
{

    return ksUpdateSub(param, 2);

}

int ksSelectASub(PGconn *conn, const char *cursorname)
{

    PGresult *res = NULL;
    char buff[512];

    // DECLARE CURSOR

    sprintf(buff,
        "DECLARE %s CURSOR FOR "
          "SELECT "
            "rid,"
            "tid,"
            "value "
          "FROM "
            "TestPQ.F_SELECT_TEST(LOCALTIMESTAMP)", cursorname);

    res = PQexec(conn, buff);

    if (PGRES_COMMAND_OK != PQresultStatus(res)) {
        return KS_NG;
    }

    PQclear(res);
    res = NULL;

    // FETCH

    sprintf(buff, "FETCH ALL IN %s", cursorname); 
    res = PQexec(conn, buff);

    if (PGRES_TUPLES_OK != PQresultStatus(res)) {
        return KS_NG;
    }

    putc('\n', stdout);

    for (int idx = 0; idx < PQntuples(res); idx++) {
        printf("%-10s%-16s%-16s\n",
            PQgetvalue(res, idx, 0),
            PQgetvalue(res, idx, 1),
            PQgetvalue(res, idx, 2));
    }

    PQclear(res);
    res = NULL;

    return KS_OK;

}

void *ksSelectA(void *param)
{

    PGconn      *conn       = (PGconn*)param;
    PGresult    *res        = NULL;
    long        retval      = KS_OK;

    do {

        // BEGIN TRANSACTION

        res = PQexec(conn, KS_BEGINTRANSACTION_READONLY);

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

        PQclear(res);
        res = NULL;

        if (KS_OK != (retval = ksSelectASub(conn, "cursor1"))) {
            break;
        }

        KS_SLEEP(400);

        if (KS_OK != (retval = ksSelectASub(conn, "cursor2"))) {
            break;
        }

        // END TRANSACTION

        res = PQexec(conn, "END TRANSACTION");

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

    } while(0);

    if (res) {
        PQclear(res);
        res = NULL;
    }

    putc('\n', stdout);

    return ((void*)retval);

}

int ksSelectBSub(PGconn *conn)
{

    PGresult *res = NULL;

    if (0 == PQsendQuery(conn,
        "SELECT "
          "rid,"
          "tid,"
          "value "
        "FROM "
          "TestPQ.F_SELECT_TEST(LOCALTIMESTAMP)"
        )) {

        return KS_NG;

    }

    putc('\n', stdout);

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

    return KS_OK;

}

void *ksSelectB(void *param)
{

    PGconn      *conn       = (PGconn*)param;
    PGresult    *res        = NULL;
    long        retval      = KS_OK;

    do {

        // BEGIN TRANSACTION

        res = PQexec(conn, KS_BEGINTRANSACTION_READONLY);

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

        PQclear(res);
        res = NULL;

        // SELECT

        if (KS_OK != (retval = ksSelectBSub(conn))) {
            break;
        }

        KS_SLEEP(500);

        if (KS_OK != (retval = ksSelectBSub(conn))) {
            break;
        }

        // END TRANSACTION

        res = PQexec(conn, "END TRANSACTION");

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

    } while(0);

    if (res) {
        PQclear(res);
        res = NULL;
    }

    putc('\n', stdout);

    return ((void*)retval);

}

int main(int argc, const  char* argv[])
{

    int             ret         = KS_OK;
    PGconn          *conns      = NULL;
    PGconn          *connu1     = NULL;
    PGconn          *connu2     = NULL;
    pthread_t       ths         = 0;
    pthread_t       thu1        = 0;
    pthread_t       thu2        = 0;
    void            *rths       = NULL;
    void            *rthu1      = NULL;
    void            *rthu2      = NULL;
    ksInsertContext ictx        [KS_INSERT_COUNT];

    for (int idx = 0; idx < KS_INSERT_COUNT; idx++) {

        ictx[idx].pth       = 0;
        ictx[idx].retval    = KS_OK;
        ictx[idx].conn      = NULL;
        ictx[idx].data      = ksInsertData[idx];

    }

    do {

        for (int idx = 0; idx < KS_INSERT_COUNT; idx++) {

            if (KS_OK != (ret = ksConnect(&ictx[idx].conn))) {
                break;
            }

            ictx[idx].data = ksInsertData[idx];

        }

        if (ret != KS_OK) {
            break;
        }

        if (KS_OK != (ret = ksConnect(&conns))  ||
            KS_OK != (ret = ksConnect(&connu1)) ||
            KS_OK != (ret = ksConnect(&connu2))) {
            break;
        }

        if (0 != pthread_create(&ths, NULL, ksSelectB, (void*)conns)) {
            break;
        }

        if (0 != pthread_create(&thu1, NULL, ksUpdate1,  (void*)connu1)) {
            break;
        }

        // if (0 != pthread_create(&thu2, NULL, ksUpdate2,  (void*)connu2)) {
        //    break;
        // }

        for (int idx = 0; idx < KS_INSERT_COUNT; idx++) {
            if (0 != pthread_create(&ictx[idx].pth, NULL, ksInsert,  (void*)&ictx[idx])) {
                break;
            }
        }

    } while(0);

    for (int idx = 0; idx < KS_INSERT_COUNT; idx++) {

        if (ictx[idx].pth) {
            pthread_join(ictx[idx].pth, (void*)(&ictx[idx].retval));
            if (ret == KS_OK) {
                ret = ictx[idx].retval;
            }
        }

        if (ictx[idx].conn) {
            PQfinish(ictx[idx].conn);
        }

    }

    if (ths) {
        pthread_join(ths, &rths);
        if (ret == KS_OK) {
            ret = *((long*)&rths);
        }
    }

    if (thu1) {
        pthread_join(thu1, &rthu1);
        if (ret == KS_OK) {
            ret = *((long*)&rthu1);
        }
    }

    if (thu2) {
        pthread_join(thu2, &rthu2);
        if (ret == KS_OK) {
            ret = *((long*)&rthu2);
        }
    }

    if (conns) {
        PQfinish(conns);
    }

    if (connu1) {
        PQfinish(connu1);
    }

    if (connu2) {
        PQfinish(connu2);
    }

    return ret;

}

