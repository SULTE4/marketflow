package model

type Exchange struct {
	Symbol    string  `json:"symbol"`
	Price     float32 `json:"price"`
	Timestamp int64   `json:"timestamp"`
	Source string 
}

func NewExchange(lines [][]byte) Exchange {
	return Exchange{Symbol: ""}
}
