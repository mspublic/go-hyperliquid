package hyperliquid

//go:generate easyjson -all

type BboSubscriptionParams struct {
	Coin string
}

func (w *WebsocketClient) Bbo(
	params BboSubscriptionParams,
	callback func(Bbo, error),
) (*Subscription, error) {
	remotePayload := remoteBboSubscriptionPayload{
		Type: ChannelBbo,
		Coin: params.Coin,
	}

	return w.subscribe(remotePayload, func(msg any) {
		bbo, ok := msg.(Bbo)
		if !ok {
			callback(Bbo{}, ErrInvalidMessageType)
			return
		}

		callback(bbo, nil)
	})
}
