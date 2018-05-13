package models

type SystemModel struct {
	DateTime    string
	Close       float64
	Volume      int
	Diff        float64
	Pct         float64
	CloseToLow  float64
	HighToClose float64
	BuySide     int
	SeellSide   int
}

type PerformanceModel struct {
	DateTime string
	//pass
}

type IndicatorModel struct {
	DateTime string
	//pass
}

type DataModel struct {
	DateTime string
	//pass
}
