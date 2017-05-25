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

extern int divider = 3;
extern int start   = 0;
extern int end     = 1;

#include <Symbols.mqh>

static string   sSymbols[100];
static int      iSymbols;
static datetime tPreviousTime;
int             DB; // database identifier
int             s;
int             i;
string          sPeriod = "," + PeriodToStr();
string          open, high, low, close, volume, date_time;

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

    if (Refresh() == true)
    {
        DoExport();
    }

//----
    return(0);
}
//+------------------------------------------------------------------+

//update base only once a bar
bool Refresh()
{
    static datetime PrevBar;

    if (PrevBar != iTime(NULL, Period(), 0))
    {
        PrevBar = iTime(NULL, Period(), 0);
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
    int    filehandle;

    for (s = start * iSymbols; s <= iSymbols * end; s++)
    {
        string symbol  = make_symbol(sSymbols[s]);
        string company = AccountCompany();
        int    period;

        for (int p = 0; p <= 2; p++)
        {
            if (p == 0)
            {
                int day = 1440;

                if (StringLen(symbol) > 0 && StringLen(company))
                {
                    filehandle = FileOpen("DATA_MODEL_" + company + "_" + symbol + "_1440.csv", FILE_WRITE | FILE_CSV);
                }
                else
                {
                    filehandle = -999;
                }

                for (i = 0; i <= iBars(sSymbols[s], day) - 1; i++)
                {
                    date_time = TimeToStr(iTime(sSymbols[s], day, i), TIME_DATE | TIME_MINUTES | TIME_SECONDS);
                    open      = DoubleToStr(iOpen(sSymbols[s], day, i), 5);
                    high      = DoubleToStr(iHigh(sSymbols[s], day, i), 5);
                    low       = DoubleToStr(iLow(sSymbols[s], day, i), 5);
                    close     = DoubleToStr(iClose(sSymbols[s], day, i), 5);
                    volume    = IntegerToString(iVolume(sSymbols[s], day, i));
                    string buffer;

                    if (i == 0)
                    {
                        if (filehandle != -999)
                        {
                            buffer = "DATE_TIME,OPEN,HIGH,LOW,CLOSE, VOLUME";
                            FileWrite(filehandle, buffer);

                            buffer = date_time + "," + open + "," + high + "," + low + "," + close + "," + volume;
                            FileWrite(filehandle, buffer);
                        }
                    }
                    else
                    {
                        if (filehandle != -999)
                        {
                            if (close != 0 && open != 0 && high != 0 && low != 0)
                            {
                                buffer = date_time + "," + open + "," + high + "," + low + "," + close + "," + volume;

                                FileWrite(filehandle, buffer);
                            }
                        }
                    }
                }
                if (filehandle != INVALID_HANDLE)
                {
                    FileClose(filehandle);
                    Print("Created file for " + symbol);
                }
                else
                {
                    Print("Error in FileOpen. Error code=", GetLastError());
                }
            }

            if (p == 1)
            {
                int week = 43200;

                if (StringLen(symbol) > 0 && StringLen(company))
                {
                    filehandle = FileOpen("DATA_MODEL_" + company + "_" + symbol + "_43200.csv", FILE_WRITE | FILE_CSV);
                }
                else
                {
                    filehandle = -999;
                }

                for (i = 0; i <= iBars(sSymbols[s], week) - 1; i++)
                {
                    date_time = TimeToStr(iTime(sSymbols[s], week, i), TIME_DATE | TIME_MINUTES | TIME_SECONDS);
                    open      = DoubleToStr(iOpen(sSymbols[s], week, i), 5);
                    high      = DoubleToStr(iHigh(sSymbols[s], week, i), 5);
                    low       = DoubleToStr(iLow(sSymbols[s], week, i), 5);
                    close     = DoubleToStr(iClose(sSymbols[s], week, i), 5);
                    volume    = IntegerToString(iVolume(sSymbols[s], week, i));
                    buffer    = "";

                    if (i == 0)
                    {
                        if (filehandle != -999)
                        {
                            buffer = "DATE_TIME,OPEN,HIGH,LOW,CLOSE, VOLUME";
                            FileWrite(filehandle, buffer);

                            buffer = date_time + "," + open + "," + high + "," + low + "," + close + "," + volume;
                            FileWrite(filehandle, buffer);
                        }
                    }
                    else
                    {
                        if (filehandle != -999)
                        {
                            if (close != 0 && open != 0 && high != 0 && low != 0)
                            {
                                buffer = date_time + "," + open + "," + high + "," + low + "," + close + "," + volume;

                                FileWrite(filehandle, buffer);
                            }
                        }
                    }
                }
                if (filehandle != INVALID_HANDLE)
                {
                    FileClose(filehandle);
                    Print("Created file for " + symbol);
                }
                else
                {
                    Print("Error in FileOpen. Error code=", GetLastError());
                }
            }

            if (p == 2)
            {
                int month = 10080;

                if (StringLen(symbol) > 0 && StringLen(company))
                {
                    filehandle = FileOpen("DATA_MODEL_" + company + "_" + symbol + "_10080.csv", FILE_WRITE | FILE_CSV);
                }
                else
                {
                    filehandle = -999;
                }

                for (i = 0; i <= iBars(sSymbols[s], month) - 1; i++)
                {
                    date_time = TimeToStr(iTime(sSymbols[s], month, i), TIME_DATE | TIME_MINUTES | TIME_SECONDS);
                    open      = DoubleToStr(iOpen(sSymbols[s], month, i), 5);
                    high      = DoubleToStr(iHigh(sSymbols[s], month, i), 5);
                    low       = DoubleToStr(iLow(sSymbols[s], month, i), 5);
                    close     = DoubleToStr(iClose(sSymbols[s], month, i), 5);
                    volume    = IntegerToString(iVolume(sSymbols[s], month, i));
                    buffer    = "";

                    if (i == 0)
                    {
                        if (filehandle != -999)
                        {
                            buffer = "DATE_TIME,OPEN,HIGH,LOW,CLOSE, VOLUME";
                            FileWrite(filehandle, buffer);

                            buffer = date_time + "," + open + "," + high + "," + low + "," + close + "," + volume;
                            FileWrite(filehandle, buffer);
                        }
                    }
                    else
                    {
                        if (filehandle != -999)
                        {
                            if (close != 0 && open != 0 && high != 0 && low != 0)
                            {
                                buffer = date_time + "," + open + "," + high + "," + low + "," + close + "," + volume;

                                FileWrite(filehandle, buffer);
                            }
                        }
                    }
                }
                if (filehandle != INVALID_HANDLE)
                {
                    FileClose(filehandle);
                    Print("Created file for " + symbol);
                }
                else
                {
                    Print("Error in FileOpen. Error code=", GetLastError());
                }
            }
        } //for periods
    }     // for symbols
}
