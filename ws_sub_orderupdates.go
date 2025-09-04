package hyperliquid

//go:generate easyjson -all

type OrderUpdatesSubscriptionParams struct {
	User string
}

func (w *WebsocketClient) OrderUpdates(
	params OrderUpdatesSubscriptionParams,
	callback func([]WsOrder, error),
) (*Subscription, error) {
	payload := remoteOrderUpdatesSubscriptionPayload{
		Type: ChannelOrderUpdates,
		User: params.User,
	}

	return w.subscribe(payload, func(msg any) {
		orders, ok := msg.(WsOrders)
		if !ok {
			callback(nil, ErrInvalidMessageType)
			return
		}

		callback([]WsOrder(orders), nil)
	})
}
