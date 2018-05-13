package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"./database"
	"./models"

	"github.com/die-net/lrucache"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
)

var cache = lrucache.New(104857600, 10800) //100 Mb, 3 hours

func init() {
	//note, this package isn't fully compatible with pydotenv!
	// TODO separate envs in appropriate folders finally
	err := godotenv.Load("../.env")
	if err != nil {
		log.Fatal("Error loading environment variables.")
	}
}

func main() {
	Host := os.Getenv("API_HOST")

	app := mux.NewRouter()
	//app.Host(Host)

	app.HandleFunc("/", RedirectHandler)
	app.HandleFunc("/symbols/", SymbolsHandler)
	app.HandleFunc("/brokers/", BrokersHandler)
	app.HandleFunc("/systems/", SystemsHandler)
	app.HandleFunc("/periods/", PeriodsHandler)
	app.HandleFunc("/stats/{broker_slug}/{symbol}/{period}/{strategy}/{direction}/", StatsHandler)
	app.HandleFunc("/indexes/ai50/{broker_slug}/components/", IndexHandler)
	app.HandleFunc("/indexes/ai50/{broker_slug}/latest/{days}/", LatestHandler)
	app.HandleFunc("/indexes/ai50/{broker_slug}/results/", ResultsHandler)
	app.HandleFunc("/strategy/{broker_slug}/{symbol}/{period}/{strategy}/", StrategyHandler)

	server := &http.Server{
		Handler:      app,
		Addr:         Host + ":" + os.Getenv("API_PORT"),
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(server.ListenAndServe())

}

/*

async def slugifier(what):
    str = str(slugify(what)).replace('-', '_')
    return str

async def api_ai50(request, broker_slug):
    try:
        from collector.tasks import qindex

        broker_slug = force_text(broker_slug, encoding='utf-8', strings_only=True, errors='strict')
        s = qindex(broker=broker_slug)

        return json({'data': s})
    except:
        raise NotFound("Not foud.")


async def api_ai50_latest(request, days, broker_slug):
    try:
        from collector.models import Signals

        days = force_text(days, encoding='utf-8', strings_only=True, errors='strict')
        dlt = datetime.now() - timedelta(days=int(days))
        broker_slug = force_text(broker_slug, encoding='utf-8', strings_only=True, errors='strict')

        s = Signals.objects.filter(user__id=1, broker__slug=broker_slug, date_time__gte=dlt).order_by('date_time').reverse()

        data = serializers.serialize('json', s, fields=("date_time", "broker__title", \
            "symbol__symbol", "period__period", "system__title", "direction"))

        return json({'data': data})
    except:
        raise NotFound("Not foud.")


async def get_limit_from(period):
    if period == '1440':
        limit_from = datetime.now() - timedelta(days=2)
    if period == '10080':
        limit_from = datetime.now() - timedelta(days=7)
    if period == '43200':
        limit_from = datetime.now() - timedelta(days=28)
    return limit_from

async def autoportfolio_index(request, broker_slug):
    try:
        from collector.tasks import read_df

        broker_slug = force_text(broker_slug, encoding='utf-8', strings_only=True, errors='strict')

        filename = join(settings.DATA_PATH, 'portfolios', '{}_qndx.mp'.format(broker_slug))
        df = await read_df(filename=filename)

        #limit_from = await get_limit_from(period=period)
        #df = df.loc[df.index < limit_from]
        data = {
            'data': df.to_dict(orient='index')
        }
        return json(data)
    except:
        raise NotFound("Not foud.")

async def api_strategy(request, broker_slug, symbol, period, strategy, key):
    try:
        from collector.tasks import asy_get_df

        broker_slug = force_text(broker_slug, encoding='utf-8', strings_only=True, errors='strict')
        symbol = force_text(symbol, encoding='utf-8', strings_only=True, errors='strict')
        period = force_text(period, encoding='utf-8', strings_only=True, errors='strict')
        strategy = force_text(strategy, encoding='utf-8', strings_only=True, errors='strict')

        df = await asy_get_df(con=await conn(), broker_slug=broker_slug, symbol=symbol, period=period, system=strategy, folder='systems', limit=False)
        try:
            del df['PCT']
            del df['hc']
            del df['cl']
            del df['VALUE']
            del df['DIFF']
        except:
            pass

        if key:
            try:
                from collector.models import QtraUser
                key = force_text(key, encoding='utf-8', strings_only=True, errors='strict')
                user = QtraUser.objects.filter(key=key)
                if not ((user[0].user_type == 1) & (len(user) > 0)):
                    limit_from = await get_limit_from(period=period)
                    df = df.loc[df.index < limit_from]
            except:
                limit_from = await get_limit_from(period=period)
                df = df.loc[df.index < limit_from]
        else:
            limit_from = await get_limit_from(period=period)
            df = df.loc[df.index < limit_from]

        data = {
            'data': df.to_dict(orient='index')
        }
        return json(data)
    except:
        raise NotFound("Not foud.")

*/

func SymbolsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	cached, isCached := cache.Get("symbols")
	if isCached == false {
		db := database.Connect()
		defer db.Close()

		query := `SELECT id,
			COALESCE(NULLIF(symbol, NULL), ''),
			COALESCE(NULLIF(description, NULL), ''),
			COALESCE(NULLIF(currency, NULL), ''),
			COALESCE(NULLIF(sources, NULL), -1),
			COALESCE(NULLIF(digits, NULL), -1),
			COALESCE(NULLIF(profit_type, NULL), -1),
			COALESCE(NULLIF(spread, NULL), -1),
			COALESCE(NULLIF(tick_value, NULL), -1),
			COALESCE(NULLIF(tick_size, NULL), -1),
			COALESCE(NULLIF(price, NULL), -1),
			COALESCE(NULLIF(commission, NULL), -1),
			COALESCE(NULLIF(margin_initial, NULL), -1),
			broker_id FROM collector_symbols;`

		rows, err := db.Query(query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		symbols := make([]models.Symbol, 0)
		for rows.Next() {
			symbol := models.Symbol{}
			err := rows.Scan(&symbol.ID, &symbol.Symbol, &symbol.Description, &symbol.Currency, &symbol.Sources.ID,
				&symbol.Digits, &symbol.ProfitType, &symbol.Spread, &symbol.TickValue, &symbol.TickSize,
				&symbol.Price, &symbol.Commission, &symbol.MarginInitial, &symbol.Broker.ID)
			if err != nil {
				panic(err)
			}
			symbols = append(symbols, symbol)
		}
		if err = rows.Err(); err != nil {
			panic(err)
		}

		j, err := json.Marshal(symbols)
		if err != nil {
			log.Fatal(err)
		}

		cache.Set("symbols", j)
		w.Write(j)
	}
	w.Write(cached)
}

func BrokersHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	cached, isCached := cache.Get("brokers")
	if isCached == false {
		db := database.Connect()
		defer db.Close()

		query := `SELECT id, title, 
			COALESCE(NULLIF(description, NULL), ''),
			COALESCE(NULLIF(slug, NULL), ''),
			COALESCE(NULLIF(registration_url, NULL), '') FROM collector_brokers;`
		rows, err := db.Query(query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		brokers := make([]models.Broker, 0)
		for rows.Next() {
			broker := models.Broker{}
			err := rows.Scan(&broker.ID, &broker.Title, &broker.Description, &broker.Slug, &broker.RegistrationUrl)
			if err != nil {
				panic(err)
			}
			brokers = append(brokers, broker)
		}
		if err = rows.Err(); err != nil {
			panic(err)
		}

		j, err := json.Marshal(brokers)
		if err != nil {
			log.Fatal(err)
		}

		cache.Set("brokers", j)
		w.Write(j)
	}
	w.Write(cached)
}

func PeriodsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	cached, isCached := cache.Get("periods")
	if isCached == false {
		db := database.Connect()
		defer db.Close()

		query := `SELECT * FROM collector_periods;`
		rows, err := db.Query(query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		periods := make([]models.Period, 0)
		for rows.Next() {
			period := models.Period{}
			err := rows.Scan(&period.ID, &period.Period, &period.Name)
			if err != nil {
				panic(err)
			}
			periods = append(periods, period)
		}
		if err = rows.Err(); err != nil {
			panic(err)
		}

		j, err := json.Marshal(periods)
		if err != nil {
			log.Fatal(err)
		}

		cache.Set("periods", j)
		w.Write(j)
	}
	w.Write(cached)
}

func SystemsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	cached, isCached := cache.Get("systems")
	if isCached == false {
		db := database.Connect()
		defer db.Close()

		query := `SELECT id, title,
			COALESCE(NULLIF(description, NULL), ''),
			COALESCE(NULLIF(slug, NULL), ''),
			COALESCE(NULLIF(indicator_id, NULL), -1) FROM collector_systems;`
		rows, err := db.Query(query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		systems := make([]models.System, 0)
		for rows.Next() {
			system := models.System{}
			err := rows.Scan(&system.ID, &system.Title, &system.Description, &system.Slug, &system.Indicator.ID)
			if err != nil {
				panic(err)
			}
			systems = append(systems, system)
		}
		if err = rows.Err(); err != nil {
			panic(err)
		}

		j, err := json.Marshal(systems)
		if err != nil {
			log.Fatal(err)
		}

		cache.Set("systems", j)
		w.Write(j)
	}
	w.Write(cached)
}

func StatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	broker := strings.Split(r.RequestURI, "/")[2]
	symbol := strings.Split(r.RequestURI, "/")[3]
	period := strings.Split(r.RequestURI, "/")[4]
	system := strings.Split(r.RequestURI, "/")[5]
	direction := strings.Split(r.RequestURI, "/")[6]

	cached, isCached := cache.Get("stats_" + broker + "_" + symbol + "_" + period + "_ " + system + "_" + direction)
	if isCached == false {
		db := database.Connect()
		defer db.Close()

		query := fmt.Sprintf(`SELECT id, broker_id, symbol_id, period_id, system_id, direction,
			COALESCE(NULLIF(intraday_dd, NULL), -1),
			COALESCE(NULLIF(max_dd, NULL), -1),
			COALESCE(NULLIF(sharpe, NULL), -1),
			COALESCE(NULLIF(bh_sharpe, NULL), -1),
			COALESCE(NULLIF(sortino, NULL), -1),
			COALESCE(NULLIF(bh_sortino, NULL), -1),
			COALESCE(NULLIF(std, NULL), -1),
			COALESCE(NULLIF(var, NULL), -1),
			COALESCE(NULLIF(avg_trade, NULL), -1),
			COALESCE(NULLIF(avg_win, NULL), -1),
			COALESCE(NULLIF(avg_loss, NULL), -1),
			COALESCE(NULLIF(win_rate, NULL), -1),
			COALESCE(NULLIF(trades, NULL), -1),
			COALESCE(NULLIF(fitness, NULL), -1),
			COALESCE(NULLIF(total_profit, NULL), -1),
			COALESCE(NULLIF(acc_minimum, NULL), -1),
			COALESCE(NULLIF(yearly, NULL), -1),
			COALESCE(NULLIF(yearly_p, NULL), -1),
			COALESCE(NULLIF(strategy_url, NULL), ''),
			COALESCE(NULLIF(heatmap, NULL), ''),
			COALESCE(NULLIF(img, NULL), ''),
			COALESCE(NULLIF(yearly_ret, NULL), '') FROM collector_stats WHERE (broker_id='%s') 
			AND (symbol_id='%s') AND (period_id='%s') AND (system_id='%s') AND 
			(direction='%s');`, broker, symbol, period, system, direction)

		rows, err := db.Query(query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		stats := make([]models.Stats, 0)
		for rows.Next() {
			stat := models.Stats{}
			err := rows.Scan(&stat.ID, &stat.Broker.ID, &stat.Symbol.ID, &stat.Period.ID, &stat.System.ID, &stat.Direction.ID,
				&stat.IntradayDD, &stat.MaxDD, &stat.Sharpe, &stat.SharpeBuyHold, &stat.Sortino, &stat.SortinoBuyHold,
				&stat.Std, &stat.Var, &stat.AvgTrade, &stat.AvgWin, &stat.AvgLoss, &stat.WinRate, &stat.Trades,
				&stat.Fitness, &stat.TotalProfit, &stat.AccMinimum, &stat.Yearly, &stat.YearlyInPerc, &stat.StrategyUrl,
				&stat.Heatmap, &stat.StrategyImg, &stat.YearlyReturnImg)
			if err != nil {
				panic(err)
			}
			stats = append(stats, stat)
		}
		if err = rows.Err(); err != nil {
			panic(err)
		}

		j, err := json.Marshal(stats)
		if err != nil {
			log.Fatal(err)
		}

		cache.Set("stats_"+broker+"_"+symbol+"_"+period+"_ "+system+"_"+direction, j)
		w.Write(j)
	}
	w.Write(cached)
}

func StrategyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	broker := strings.Split(r.RequestURI, "/")[2]
	symbol := strings.Split(r.RequestURI, "/")[3]
	period := strings.Split(r.RequestURI, "/")[4]
	system := strings.Split(r.RequestURI, "/")[5]

	cached, isCached := cache.Get("strategy_" + broker + "_" + symbol + "_" + period + "_ " + system)
	if isCached == false {

		//get data file
		_, currentFilePath, _, _ := runtime.Caller(0)
		basePath := path.Dir(currentFilePath)
		broker, _ := url.PathUnescape(broker)
		file := broker + "==" + symbol + "==" + period + "==" + system + ".json"
		//dataFile := path.Join(basePath, "../", "data", "systems", "json", file)
		if _, err := os.Stat(path.Join(basePath, "../", "data", "systems", "json", file)); !os.IsNotExist(err) {
			data, err := ioutil.ReadFile(path.Join(basePath, "../", "data", "systems", "json", file))
			if err != nil {
				log.Fatal(err)
				data = []byte("")
			}

			cache.Set("strategy_"+broker+"_"+symbol+"_"+period+"_ "+system, data)
			w.Write(data)
		}
	}
	w.Write(cached)
}

/*======================================================================================================*/
/*======================================================================================================*/
/*======================================================================================================*/

func IndexHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	//app.HandleFunc("/indexes/ai50/{broker_slug}/components/", IndexHandler)
}

func LatestHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	//app.HandleFunc("/indexes/ai50/{broker_slug}/latest/{days}/", LatestHandler)
}

func ResultsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	//app.HandleFunc("/indexes/ai50/{broker_slug}/results/", ResultsHandler)
}

func RedirectHandler(w http.ResponseWriter, r *http.Request) {
	tpl := template.Must(template.ParseFiles("templates/redirect.html"))

	err := tpl.Execute(w, "no data")
	if err != nil {
		log.Fatal(err)
	}
}
