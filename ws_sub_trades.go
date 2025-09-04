package hyperliquid

//go:generate easyjson -all

type TradesSubscriptionParams struct {
	Coin string
}

func (w *WebsocketClient) Trades(
	params TradesSubscriptionParams,
	callback func([]Trade, error),
) (*Subscription, error) {
	remotePayload := remoteTradesSubscriptionPayload{
		Type: ChannelTrades,
		Coin: params.Coin,
	}

	return w.subscribe(remotePayload, func(msg any) {
		trades, ok := msg.(Trades)
		if !ok {
			callback(nil, ErrInvalidMessageType)
			return
		}

		callback(trades, nil)
	})
}
