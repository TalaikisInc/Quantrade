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

//dev
//extern string host     = "localhost";
//server
extern string host     = "";

extern int    port     = 3306;
extern int    socket   = 0;
extern string user     = "root";

//dev
//extern string password = "";
//server
extern string password = "";

extern string dbName   = "admin_quantrade";

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
        iSymbols = Symbols(sSymbols);

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
    int final_;
    final_  = StringReplace(text, "_", "");
    final_ += StringReplace(text, "#", "");
    final_ += StringReplace(text, ".", "");
    final_ += StringReplace(text, "&", "");
    final_ += StringReplace(text, "-", "");

    return(text);
}

void DoExport()
{
    string Query;
    int    i, Cursor, Rows;

    Query = "START TRANSACTION;";

    if (MySqlExecute(DB, Query))
    {
        Print("Succeeded: ", Query);
    }
    else
    {
        //Print("Error: ", MySqlErrorDescription, " with: ", Query);
    }

    for (s = 0; s <= iSymbols; s++)
    {
        //for each symbol
        double spread         = NormalizeDouble(MarketInfo(sSymbols[s], MODE_SPREAD), 5);
        double tick_value     = NormalizeDouble(MarketInfo(sSymbols[s], MODE_TICKVALUE), 5);
        double tick_size      = NormalizeDouble(MarketInfo(sSymbols[s], MODE_TICKSIZE), 5);
        double margin_initial = NormalizeDouble(MarketInfo(sSymbols[s], MODE_MARGINREQUIRED), 2);
        double digits         = NormalizeDouble(MarketInfo(sSymbols[s], MODE_DIGITS), 2);
        double profit_mode    = NormalizeDouble(MarketInfo(sSymbols[s], MODE_PROFITCALCMODE), 2);
        double points         = NormalizeDouble(MarketInfo(sSymbols[s], MODE_POINT), 2);

        string profit_currency    = SymbolInfoString(sSymbols[s], SYMBOL_CURRENCY_PROFIT);
        string description        = SymbolInfoString(sSymbols[s], SYMBOL_DESCRIPTION);
        double price_at_calc_time = iClose(sSymbols[s], 1440, 1);
        string to_find            = make_symbol(sSymbols[s]);
        string  company = AccountCompany();

        if (StringLen(to_find) > 0 && StringLen(company) > 0)
        {
            //Print(company);
            //Print(to_find);

            Query = "UPDATE collector_symbols SET spread=" + spread + ", tick_value=" + tick_value +
                ", tick_size=" + tick_size + ", margin_initial=" + margin_initial + ", digits=" + digits +
                ", profit_calc=" + profit_mode + ", description='" + description + "', profit_currency='" +
                profit_currency + "', price_at_calc_time=" + price_at_calc_time + ", points=" + points +
                " WHERE symbol='" + to_find + "' AND broker='"+ company +"';";

            if (MySqlExecute(DB, Query))
            {
                Print("Succeeded: ", Query);
            }
            else
            {
                //Print("Error: ", MySqlErrorDescription, " with: ", Query);
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
        //Print("Error: ", MySqlErrorDescription, " with: ", Query);
    }
}
