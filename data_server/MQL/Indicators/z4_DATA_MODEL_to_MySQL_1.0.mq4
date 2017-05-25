/*
 * Developed by Quantrade Ltd.
 * QUANTRADE.CO.UK
 * Copyright 2016 Quantrade Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */

#property version "1.0"
#property indicator_chart_window

//extern string host     = "";
// dev
extern string host   = "localhost";
extern int    port   = 3306;
extern int    socket = 0;
extern string user   = "root";
//extern string password = "";
extern string password = "";
extern string dbName   = "admin_quantrade";
extern int    divider  = 3;
extern int    start    = 0;
extern int    end      = 1;
extern bool   ParseAll = False;

#include <MQLMySQL.mqh>
#include <Symbols.mqh>

static string   sSymbols[100];
static int      iSymbols;
static datetime tPreviousTime;
int             DB; // database identifier
int             s;
int             i;
string          sPeriod = "," + PeriodToStr();

//+------------------------------------------------------------------+
//| Custom indicator initialization function                         |
//+------------------------------------------------------------------+

int init()
{
//---- indicators

//----
    return(0);
}
//+------------------------------------------------------------------+
//| Custom indicator deinitialization function                       |
//+------------------------------------------------------------------+
int deinit()
{
//----

//----
    return(0);
}
//+------------------------------------------------------------------+
//| Custom indicator iteration function                              |
//+------------------------------------------------------------------+
int start()
{
    int bars = IndicatorCounted() - 1;

    // only load the Symbols once into the array "sSymbols"
    if (iSymbols == 0)
        iSymbols = Symbols(sSymbols) / divider;

    //Print (MySqlVersion());

    if (Refresh(Period()) == true)
    {
        // open database connection
        Print("Connecting...");

        //connect to database
        DB = MySqlConnect(host, user, password, dbName, port, socket, CLIENT_MULTI_STATEMENTS);

        if (DB == -1)
        {
            Print("Connection to MySQL database failed! Error: " + MySqlErrorDescription);
        }
        else
        {
            Print("Connected! DB_ID#", DB);
        }
        DoExport();
        MySqlDisconnect(DB);
        Print("MySQL disconnected. Bye.");
    }

//----
    return(0);
}
//+------------------------------------------------------------------+

//update base only once a bar
bool Refresh(int _per)
{
    static datetime PrevBar;
    //Print("Refresh times. PrevBar: "+PrevBar);

    if (PrevBar != iTime(NULL, _per, 0))
    {
        PrevBar = iTime(NULL, _per, 0);
        return(true);
    }
    else
    {
        return(false);
    }
}

string make_symbol(string text)
{
    int final;
    final  = StringReplace(text, "_", "");
    final += StringReplace(text, "#", "");
    final += StringReplace(text, ".", "");
    final += StringReplace(text, "&", "");
    final += StringReplace(text, "-", "");

    return(text);
}

void DoExport()
{
    string Query;
    int    i, Cursor, Rows;

    for (s = start * iSymbols; s <= iSymbols * end; s++)
    {
        int    period  = Period();
        string to_find = make_symbol(sSymbols[s]);
        string broker  = "AvaFx";

        Query = "SELECT date_time FROM collector_data WHERE symbol='" + to_find + "' AND period=" + period + " AND broker='" + broker + "' ORDER BY date_time DESC LIMiT 1;";

        Print("SQL> ", Query);

        Cursor = MySqlCursorOpen(DB, Query);

        if (Cursor >= 0)
        {
            Rows = MySqlCursorRows(Cursor);
            Print(Rows, " row(s) selected.");

            if (Rows > 0)
            {
                if (MySqlCursorFetchRow(Cursor))
                {
                    datetime date_time_fetched = MySqlGetFieldAsDatetime(Cursor, 0);
                    Print("Last date from database:");
                    Print(TimeToStr(date_time_fetched, TIME_DATE | TIME_MINUTES | TIME_SECONDS));
                }
            }
            else
            {
                Print("Here");
            }
            MySqlCursorClose(Cursor); // NEVER FORGET TO CLOSE CURSOR !!!
        }
        else
        {
            Print("Cursor opening failed. Error: ", MySqlErrorDescription);
            MySqlCursorClose(Cursor); // NEVER FORGET TO CLOSE CURSOR !!!
        }

        Query = "START TRANSACTION;";

        if (MySqlExecute(DB, Query))
        {
            Print("Succeeded: ", Query);
        }
        else
        {
            Print("Error: ", MySqlErrorDescription, " with: ", Query);
        }

        for (i = 1; i <= iBars(sSymbols[s], 0); i++)
        {
            //for each symbol
            if (iTime(sSymbols[s], 0, i) >= date_time_fetched || ParseAll)
            {
                string date_time = TimeToStr(iTime(sSymbols[s], 0, i), TIME_DATE | TIME_MINUTES | TIME_SECONDS);
                double close     = NormalizeDouble(iClose(sSymbols[s], 0, i), 5);
                double open      = NormalizeDouble(iOpen(sSymbols[s], 0, i), 5);
                double high      = NormalizeDouble(iHigh(sSymbols[s], 0, i), 5);
                double low       = NormalizeDouble(iLow(sSymbols[s], 0, i), 5);
                int    volume    = iVolume(sSymbols[s], 0, i);

                if (StringLen(to_find) > 0)
                {
                    Query = "INSERT INTO collector_data (date_time, symbol, open, high, low, close, period, volume, broker) VALUES ('" +
                            date_time + "', '" +
                            to_find + "', " +
                            open + ", " +
                            high + ", " +
                            low + ", " +
                            close + ", " +
                            period + ", " +
                            volume + ", '" +
                            broker + "');";


                    if (MySqlExecute(DB, Query))
                    {
                        Print("Succeeded: ", Query);
                    }
                    else
                    {
                        Print("Error: ", MySqlErrorDescription, " with: ", Query);
                    }
                }
            }
        }

        Query = "COMMIT;";

        if (MySqlExecute(DB, Query))
        {
            Print("Succeeded: ", Query);
        }
        else
        {
            Print("Error: ", MySqlErrorDescription, " with: ", Query);
        }
    }
}
