#property version "1.0"
#property copyright "Copyright ? 2016, Quantrade Corp."
#property link      "https: //www.quantrade.co.uk"

#property indicator_chart_window

#property indicator_buffers 1
#property indicator_color1 clrGray

#include <MQLMySQL.mqh>

extern string email = "";
extern string key = "";
extern int hour = 20;
extern int minute = 30;

//string host     = "5.189.153.43";
string host   = "localhost";
int port   = 3306;
int    socket = 0;
string user   = "root";
//string password = "5yTTuu83-#L*W7]{R6";
string password = "H@W@f54R5gf5F#$%Y#Fy#";
string dbName   = "admin_quantrade";

datetime lastAlertTime = 0;

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
    ObjectsDeleteAll();

//----
    return(0);
}
//+------------------------------------------------------------------+
//| Custom indicator iteration function                              |
//+------------------------------------------------------------------+
int start()
{
    if (Refresh(30))
    {
        int    DB;
        string Query;
        int    Cursor, Rows;
        string  company = AccountCompany();

        //connect to database
        DB = MySqlConnect(host, user, password, dbName, port, 0, CLIENT_MULTI_STATEMENTS);

        if (DB == -1)
        {
            Print("Connection to Quantrade failed! Error: " + MySqlErrorDescription);
        }
        else
        {
            Print("Connected to Quantrade!");
        }

        Query = "(SELECT symbol, period, date_time, _signal, system FROM `collector_signals` WHERE email='"+email+"') UNION " +
                 "(SELECT symbol, period, date_time, _signal, system FROM `collector_signals` WHERE _key='"+key+"') UNION " +
                 "(SELECT symbol, period, date_time, _signal, system FROM `collector_signals` WHERE broker='"+company+"');";

        //Print("SQL> ", Query);
        Cursor = MySqlCursorOpen(DB, Query);

        if (Cursor >= 0)

        {
            Rows = MySqlCursorRows(Cursor);
            //Print(Rows, " row(s) selected.");
            if (Rows > 0)
            {
                for (int s = 0; s < Rows; s++)
                {
                    if (MySqlCursorFetchRow(Cursor))
                    {
                        string symbol = MySqlGetFieldAsString(Cursor, 0);
                        string system = MySqlGetFieldAsString(Cursor, 4);
                        int period_ = MySqlGetFieldAsInt(Cursor, 1);
                        string period;
                        string signal_name;
                        
                        if(period_ == 1440)
                        {
                            period = "D1";
                        } else
                        if(period_ == 43200)
                        {
                            period = "W1";
                        }
                         else
                        if(period_ == 10080)
                        {
                            period = "M1";
                        }
                        
                        int dte = MySqlGetFieldAsDatetime(Cursor, 2);
                        string date_time = TimeToStr(dte, TIME_DATE|TIME_MINUTES|TIME_SECONDS);
                        string date_time_signal = TimeToStr(dte, TIME_DATE);
                        string date_time_current = TimeToStr(Time[0], TIME_DATE);
                        int signal = MySqlGetFieldAsInt(Cursor, 3);
                        
                        DeleteLabel("NONE");
                        DeleteLabel("ERR");
                        DeleteLabel("SIG"+s);
                        
                        ObjectCreate("SIG"+s, OBJ_LABEL, NULL, NULL, NULL);
                        if(signal == 1)
                        {
                            signal_name = "LONG";
                            ObjectSetText("SIG"+s, ""+date_time+" "+symbol+" "+period+" "+system+" "+signal_name, 16, "Arial", clrGray);
                            
                        }
                        if(signal == 2)
                        {
                            signal_name = "SHORT";
                            ObjectSetText("SIG"+s, ""+date_time+" "+symbol+" "+period+" "+system+" "+signal_name, 16, "Arial", clrGray);
                        }
                        ObjectSet("SIG"+s, OBJPROP_CORNER, 1);
                        ObjectSet("SIG"+s, OBJPROP_XDISTANCE, 10);
                        ObjectSet("SIG"+s, OBJPROP_YDISTANCE, s*20);
                        
                        if(Hour() == hour && Minute() == minute)
                        {
                            if (date_time_signal == date_time_current && (0 == s) && lastAlertTime != Time[0])
                            {
                                Alert("Quantrade "+signal_name+" SIGNAL for "+symbol);
                                SendMail("Quantrade signal", "Quantrade "+signal_name+" SIGNAL for "+symbol);
                                SendNotification("Quantrade "+signal_name+" SIGNAL for "+symbol);
                                lastAlertTime = Time[0];
                            }
                        }
                    }
                }
            }
            else
            {
                DeleteLabel("NONE");
                DeleteLabel("ERR");
                DeleteLabel("SIG1");
                
                ObjectCreate("NONE", OBJ_LABEL, NULL, NULL, NULL);
                ObjectSetText("NONE", "No signals in your account found.", 16, "Arial", clrDimGray);
                ObjectSet("NONE", OBJPROP_CORNER, 1);
                ObjectSet("NONE", OBJPROP_XDISTANCE, 10);
                ObjectSet("NONE", OBJPROP_YDISTANCE, 10);
            }
            MySqlCursorClose(Cursor); // NEVER FORGET TO CLOSE CURSOR !!!
            Print("Quantrade closed.");
        }
        else
        {
            DeleteLabel("ERR");
            DeleteLabel("NONE");
            
            ObjectCreate("ERR", OBJ_LABEL, NULL, NULL, NULL);
            ObjectSetText("ERR", "Credentials not exist.", 16, "Arial", clrDimGray);
            ObjectSet("ERR", OBJPROP_CORNER, 1);
            ObjectSet("ERR", OBJPROP_XDISTANCE, 10);
            ObjectSet("ERR", OBJPROP_YDISTANCE, 10);
                
            //Print("Cursor opening failed. Error: ", MySqlErrorDescription);
            MySqlCursorClose(Cursor); // NEVER FORGET TO CLOSE CURSOR !!!
            Print("Quantrade closed.");
        }
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

void DeleteLabel(string name)
{
    if (-1 != ObjectFind(name))
        ObjectDelete(name);
}
